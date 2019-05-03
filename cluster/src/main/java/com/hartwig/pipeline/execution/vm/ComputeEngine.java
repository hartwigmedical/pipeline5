package com.hartwig.pipeline.execution.vm;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeRequest;
import com.google.api.services.compute.model.*;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.CloudExecutor;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.NamespacedResults;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

public class ComputeEngine implements CloudExecutor<VirtualMachineJobDefinition> {
    private final static String APPLICATION_NAME = "vm-hosted-workload";
    static final String ZONE_NAME = "europe-west4-a";

    private final Logger LOGGER = LoggerFactory.getLogger(ComputeEngine.class);

    private final Arguments arguments;
    private final Compute compute;

    ComputeEngine(final Arguments arguments, final Compute compute) {
        this.arguments = arguments;
        this.compute = compute;
    }

    public static ComputeEngine from(final Arguments arguments, final GoogleCredentials credentials) throws Exception {
        return new ComputeEngine(arguments, initCompute(credentials));
    }

    @Override
    public JobStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition) {
        String vmName = bucket.name() + "-" + jobDefinition.name();
        JobStatus status;
        try {
            if (bucketContainsFile(bucket, jobDefinition.startupCommand().successFlag(), jobDefinition.namespacedResults())
                    || bucketContainsFile(bucket, jobDefinition.startupCommand().failureFlag(), jobDefinition.namespacedResults())) {
                LOGGER.warn("Job appears to have run already; skipping");
                return JobStatus.SKIPPED;
            }

            Instance instance = new Instance();
            LOGGER.info("Initialising [{}], output will go to bucket [{}]", vmName, bucket.name());
            instance.setName(vmName);
            instance.setZone(ZONE_NAME);
            String project = arguments.project();
            instance.setMachineType(machineType(ZONE_NAME, jobDefinition.performanceProfile().uri(), project));

            addServiceAccount(instance);
            attachDisk(compute, instance, jobDefinition.imageFamily(), project, vmName, jobDefinition.performanceProfile().diskGb());
            addStartupCommand(instance, bucket, jobDefinition.startupCommand());
            addNetworkInterface(instance, project);

            deleteOldInstancesAndStart(compute, instance, project, vmName);
            LOGGER.info("Successfully initialised [{}]", this);
            status = waitForCompletion(bucket, jobDefinition);
            stop(project, vmName);
        } catch (Exception e) {
            String message = format("An error occurred running job on compute engine [%s]", vmName);
            LOGGER.error(message, e);
            return JobStatus.FAILED;
        }
        return status;
    }

    private static Compute initCompute(final GoogleCredentials credentials) throws Exception {
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

    private void addStartupCommand(Instance instance, RuntimeBucket runtimeBucket, BashStartupScript startupCommand) {
        Metadata startupMetadata = new Metadata();
        Metadata.Items items = new Metadata.Items();
        items.setKey("startup-script");
        items.setValue(startupCommand.asUnixString());
        startupMetadata.setItems(singletonList(items));
        runtimeBucket.bucket().create("copy_of_startup_script_used_for_this_run.sh", startupCommand.asUnixString().getBytes());
        instance.setMetadata(startupMetadata);
    }

    private Image resolveLatestImage(Compute compute, String sourceImageFamily, String projectName) throws IOException {
        Compute.Images.GetFromFamily images = compute.images().getFromFamily(projectName, sourceImageFamily);
        Image image = images.execute();
        if (image != null) {
            LOGGER.info("Resolved image [{} ({})] ", image.getName(), image.getSelfLink());
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

    private boolean bucketContainsFile(RuntimeBucket bucket, String filename, NamespacedResults namespacedResults) {
        Page<Blob> objects = bucket.bucket().list();
        for (Blob blob : objects.iterateAll()) {
            if (blob.getName().equals(namespacedResults.path(filename))) {
                return true;
            }
        }
        return false;
    }

    private JobStatus waitForCompletion(RuntimeBucket bucket, VirtualMachineJobDefinition jobDefinition) {
        LOGGER.info("Waiting for job completion");
        while (true) {
            if (bucketContainsFile(bucket, jobDefinition.startupCommand().successFlag(), jobDefinition.namespacedResults())) {
                return JobStatus.SUCCESS;
            } else if (bucketContainsFile(bucket, jobDefinition.startupCommand().failureFlag(), jobDefinition.namespacedResults())) {
                return JobStatus.FAILED;
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
            executeSynchronously(compute.instances().stop(projectName, ZONE_NAME, vmName), projectName);
            LOGGER.info("Stopped [{}]", this);
        } catch (Exception e) {
            String message = format("Failed to stop [%s]", this);
            LOGGER.error(message, e);
            throw new RuntimeException(message, e);
        }
    }
}
