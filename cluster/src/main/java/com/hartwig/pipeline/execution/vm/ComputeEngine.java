package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeRequest;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.AttachedDiskInitializeParams;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.CloudExecutor;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComputeEngine implements CloudExecutor<VirtualMachineJobDefinition> {
    private final static String APPLICATION_NAME = "vm-hosted-workload";
    private static final String ZONE_NAME = "europe-west4-a";

    private final Logger LOGGER = LoggerFactory.getLogger(ComputeEngine.class);

    private final GoogleCredentials credentials;
    private final Arguments arguments;
    private final Storage storage;

    public ComputeEngine(final Arguments arguments, final GoogleCredentials credentials, final Storage storage) {
        this.arguments = arguments;
        this.credentials = credentials;
        this.storage = storage;
    }

    @Override
    public JobStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition) {
        try {
            Compute compute = initCompute();

            Instance instance = new Instance();
            String vmName = bucket.name() + "-" + jobDefinition.name();
            LOGGER.info("Initialising [{}]", vmName);
            instance.setName(vmName);
            instance.setZone(ZONE_NAME);
            String project = arguments.project();
            instance.setMachineType(machineType(ZONE_NAME, jobDefinition.performanceProfile().virtualMachineType().uri(), project));

            addServiceAccount(instance);
            attachDisk(compute,
                    instance,
                    jobDefinition.imageFamily(),
                    project,
                    vmName,
                    jobDefinition.performanceProfile().virtualMachineType().diskGB());
            addStartupCommand(instance, jobDefinition.startupCommand());
            addNetworkInterface(instance, project);

            deleteOldInstancesAndStart(compute, instance, project, vmName);
            LOGGER.info("Successfully initialised [{}]", this);
            waitForCompletion(bucket.bucket().getName(), jobDefinition.completionFlagFile());
            stop(project, vmName);
        } catch (Exception e) {
            String message = format("Failed to initialise [%s]", this);
            LOGGER.error(message, e);
            return JobStatus.FAILED;
        }
        return JobStatus.SUCCESS;
    }

    private Compute initCompute() throws Exception {
        HttpTransport http = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory json = JacksonFactory.getDefaultInstance();
        return new Compute.Builder(http, json, new HttpCredentialsAdapter(credentials)).setApplicationName(APPLICATION_NAME).build();
    }

    private void addNetworkInterface(Instance instance, String projectName) {
        NetworkInterface iface = new NetworkInterface();
        iface.setNetwork(format("%s/global/networks/default", apiBaseUrl(projectName)));
        AccessConfig config = new AccessConfig();
        config.setType("ONE_TO_ONE_NAT");
        config.setName("External NAT");
        iface.setAccessConfigs(singletonList(config));
        instance.setNetworkInterfaces(singletonList(iface));
    }

    private void attachDisk(Compute compute, Instance instance, String imageFamily, String projectName, String vmName, long diskSizeGB)
            throws IOException {
        Image sourceImage = resolveLatestImage(compute, imageFamily, projectName);
        AttachedDisk disk = new AttachedDisk();
        disk.setBoot(true);
        disk.setAutoDelete(true);
        AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
        params.setDiskSizeGb(diskSizeGB);
        params.setSourceImage(sourceImage.getSelfLink());
        disk.setInitializeParams(params);
        instance.setDisks(singletonList(disk));
        compute.instances().attachDisk(projectName, ZONE_NAME, vmName, disk);
    }

    private void addServiceAccount(Instance instance) {
        ServiceAccount account = new ServiceAccount();
        account.setEmail(arguments.serviceAccountEmail());
        account.setScopes(singletonList("https://www.googleapis.com/auth/cloud-platform"));
        instance.setServiceAccounts(singletonList(account));
    }

    private void addStartupCommand(Instance instance, String startupCommand) {
        Metadata startupMetadata = new Metadata();
        Metadata.Items items = new Metadata.Items();
        items.setKey("startup-script");
        items.setValue(startupCommand);
        startupMetadata.setItems(singletonList(items));
        instance.setMetadata(startupMetadata);
    }

    private Image resolveLatestImage(Compute compute, String sourceImageFamily, String projectName) throws IOException {
        Compute.Images.GetFromFamily images = compute.images().getFromFamily(projectName, sourceImageFamily);
        Image image = images.execute();
        if (image != null) {
            return image;
        }
        throw new IllegalArgumentException(format("No image for family [%s]", sourceImageFamily));
    }

    private String apiBaseUrl(String projectName) {
        return format("https://www.googleapis.com/compute/v1/projects/%s", projectName);
    }

    private String machineType(String zone, String type, String projectName) {
        return format("%s/zones/%s/machineTypes/%s", apiBaseUrl(projectName), zone, type);
    }

    /**
     * Google's API will throw if another VM with the same name exists in the project/zone which seems
     * a pragmatic approach for us to use too.
     * <p>
     * This method depends upon all the disks attached to the instance having been initialised with their
     * <code>autoDelete</code> property set to <code>true</code>, as the disks attached by this class will
     * have been.
     * <p>
     * Note also that the VM will start as soon as it is inserted.
     */
    private void deleteOldInstancesAndStart(Compute compute, Instance instance, String projectName, String vmName) throws Exception {
        Compute.Instances.Insert insert = compute.instances().insert(projectName, ZONE_NAME, instance);
        try {
            executeSynchronously(insert, projectName);
        } catch (GoogleJsonResponseException gjre) {
            if (HttpURLConnection.HTTP_CONFLICT == gjre.getDetails().getCode()) {
                LOGGER.info("Found existing [{}] instance; deleting", vmName);
                executeSynchronously(compute.instances().delete(projectName, ZONE_NAME, vmName), projectName);
                executeSynchronously(insert, projectName);
            } else {
                throw gjre;
            }
        }
    }

    private void executeSynchronously(ComputeRequest<Operation> request, String projectName) throws Exception {
        Compute compute = initCompute();
        Operation syncOp = request.execute();
        String logId = format("Operation [%s:%s]", syncOp.getOperationType(), syncOp.getName());
        LOGGER.info("{} is executing synchronously", logId);
        while ("RUNNING".equals(fetchJobStatus(compute, syncOp.getName(), projectName))) {
            LOGGER.debug("{} not done yet", logId);
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        }

        Operation execute = compute.zoneOperations().get(projectName, ZONE_NAME, syncOp.getName()).execute();
        if (execute.getError() == null) {
            LOGGER.info("{} confirmed {}", logId, fetchJobStatus(compute, syncOp.getName(), projectName));
        } else {
            throw new RuntimeException(format("Job [%s] did not succeed: %s", syncOp.getName(), execute.toPrettyString()));
        }
    }

    private String fetchJobStatus(Compute compute, String jobName, String projectName) throws IOException {
        return compute.zoneOperations().get(projectName, ZONE_NAME, jobName).execute().getStatus();
    }

    private void waitForCompletion(String outputBucket, String completionFlagFile) {
        LOGGER.info("Waiting for job completion");
        Bucket bucket = storage.get(outputBucket);
        boolean complete = false;
        while (!complete) {
            Page<Blob> objects = bucket.list();
            for (Blob blob : objects.iterateAll()) {
                if (completionFlagFile.equals(blob.getName())) {
                    complete = true;
                } else {
                    LOGGER.debug("Flag file {} not found in bucket {}; job must not be done", completionFlagFile, outputBucket);
                }
            }
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        }
    }

    private void stop(String projectName, String vmName) {
        LOGGER.info("Stopping [{}]", this);
        try {
            executeSynchronously(initCompute().instances().stop(projectName, ZONE_NAME, vmName), projectName);
            LOGGER.info("Stopped [{}]", this);
        } catch (Exception e) {
            String message = format("Failed to stop [%s]", this);
            LOGGER.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}
