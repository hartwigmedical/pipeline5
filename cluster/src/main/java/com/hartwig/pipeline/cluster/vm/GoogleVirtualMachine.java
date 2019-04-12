package com.hartwig.pipeline.cluster.vm;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeRequest;
import com.google.api.services.compute.ComputeScopes;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.api.services.compute.model.*;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.hartwig.pipeline.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class GoogleVirtualMachine {
    private final static String APPLICATION_NAME = "vm-hosted-workload";
    private static final String ZONE_NAME = "europe-west4-a";

    private final Logger LOGGER = LoggerFactory.getLogger(GoogleVirtualMachine.class);

    private final String vmName;
    private final String imageFamily;
    private final String machineType;
    private final String startupCommand;
    private final String outputBucket;
    private String completionFlagFile;

    private final GoogleCredentials credentials;
    private final String projectName;
    private final Arguments arguments;

    private GoogleVirtualMachine(final String vmName, final String imageFamily, final String machineType,
                                 final String startupCommand, final String outputBucket,
                                 final Arguments arguments, final String completionFlagFile) {
        this.vmName = vmName;
        this.imageFamily = imageFamily;
        this.machineType = machineType;
        this.startupCommand = startupCommand;
        this.arguments = arguments;
        this.projectName = arguments.project();
        this.outputBucket = outputBucket;
        this.completionFlagFile = completionFlagFile;

        try {
            this.credentials = GoogleCredentials.fromStream(
                    new FileInputStream(arguments.privateKeyPath())).createScoped(ComputeScopes.all());
        } catch (IOException ioe) {
            throw new VmInitialisationException(
                    format("Unable to initialise credentials from [%s]", arguments.privateKeyPath()), ioe);
        }
    }

    public static GoogleVirtualMachine germline(Arguments arguments, BashStartupScript startupScript, String outputBucket) {
        return new GoogleVirtualMachine("germline", "diskimager-gatk-haplotypecaller",
                "n1-standard-1", startupScript.asUnixString(), outputBucket, arguments, startupScript.completionFlag());
    }

    public void run() throws VmInitialisationException, VmExecutionException {
        LOGGER.info("Initialising [{}]", this);
        try {
            // Create output bucket up-front as it is used in completion detection
            // This code assumes that the bucket will not yet exist and does nothing to handle the case where it does
            BucketInfo info = BucketInfo.newBuilder(outputBucket).setLocation(arguments.region()).build();
            getStorage().create(info);

            Compute compute = initCompute();

            Instance instance = new Instance();
            instance.setName(vmName);
            instance.setZone(ZONE_NAME);
            instance.setMachineType(machineType(ZONE_NAME, machineType));

            addServiceAccount(instance);
            attachDisk(compute, instance);
            addStartupCommand(instance);
            addNetworkInterface(instance);

            deleteOldInstancesAndStart(compute, instance);
            LOGGER.info("Successfully initialised [{}]", this);
            waitForCompletion();
            stop();
        } catch (Exception e) {
            String message = format("Failed to initialise [%s]", this);
            LOGGER.error(message, e);
            throw new VmInitialisationException(message, e);
        }
    }

    private Compute initCompute() throws Exception {
        HttpTransport http = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory json = JacksonFactory.getDefaultInstance();
        return new Compute.Builder(http, json, new HttpCredentialsAdapter(credentials))
                .setApplicationName(APPLICATION_NAME)
                .build();
    }

    private void addNetworkInterface(Instance instance) {
        NetworkInterface iface = new NetworkInterface();
        iface.setNetwork(format("%s/global/networks/default", apiBaseUrl()));
        AccessConfig config = new AccessConfig();
        config.setType("ONE_TO_ONE_NAT");
        config.setName("External NAT");
        iface.setAccessConfigs(asList(config));
        instance.setNetworkInterfaces(asList(iface));
    }

    private void attachDisk(Compute compute, Instance instance) throws IOException {
        Image sourceImage = resolveLatestImage(compute, imageFamily);
        AttachedDisk disk = new AttachedDisk();
        disk.setBoot(true);
        disk.setAutoDelete(true);
        AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
        params.setSourceImage(sourceImage.getSelfLink());
        disk.setInitializeParams(params);
        instance.setDisks(asList(disk));
        compute.instances().attachDisk(projectName, ZONE_NAME, vmName, disk);
    }

    private void addServiceAccount(Instance instance) {
        ServiceAccount account = new ServiceAccount();
        account.setEmail(arguments.serviceAccountEmail());
        account.setScopes(asList("https://www.googleapis.com/auth/cloud-platform"));
        instance.setServiceAccounts(asList(account));
    }

    private void addStartupCommand(Instance instance) {
        Metadata startupMetadata = new Metadata();
        Metadata.Items items = new Metadata.Items();
        items.setKey("startup-script");
        items.setValue(startupCommand);
        startupMetadata.setItems(asList(items));
        instance.setMetadata(startupMetadata);
    }

    private Image resolveLatestImage(Compute compute, String sourceImageFamily) throws IOException {
        Compute.Images.GetFromFamily images = compute.images().getFromFamily(projectName, sourceImageFamily);
        Image image = images.execute();
        if (image != null) {
            return image;
        }
        throw new VmInitialisationException(format("No image for family [%s]", sourceImageFamily));
    }

    private final String apiBaseUrl() {
        return format("https://www.googleapis.com/compute/v1/projects/%s", projectName);
    }

    private String machineType(String zone, String type) {
        return format("%s/zones/%s/machineTypes/%s", apiBaseUrl(), zone, type);
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
    private void deleteOldInstancesAndStart(Compute compute, Instance instance) throws Exception {
        Compute.Instances.Insert insert = compute.instances().insert(projectName, ZONE_NAME, instance);
        try {
            executeSynchronously(insert);
        } catch (GoogleJsonResponseException gjre) {
            if (HttpURLConnection.HTTP_CONFLICT == gjre.getDetails().getCode()) {
                LOGGER.info("Found existing [{}] instance; deleting", vmName);
                executeSynchronously(compute.instances().delete(projectName, ZONE_NAME, vmName));
                executeSynchronously(insert);
            } else {
                throw gjre;
            }
        }
    }

    private void executeSynchronously(ComputeRequest<Operation> request) throws Exception {
        Compute compute = initCompute();
        Operation syncOp = request.execute();
        String logId = format("Operation [%s:%s]", syncOp.getOperationType(), syncOp.getName());
        LOGGER.info("{} is executing synchronously", logId);
        while ("RUNNING".equals(fetchJobStatus(compute, syncOp.getName()))) {
            LOGGER.debug("{} not done yet", logId);
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        }

        Operation execute = compute.zoneOperations().get(projectName, ZONE_NAME, syncOp.getName()).execute();
        if (execute.getError() == null) {
           LOGGER.info("{} confirmed {}", logId, fetchJobStatus(compute, syncOp.getName()));
        } else {
            throw new RuntimeException(format("Job [%s] did not succeed: %s", syncOp.getName(), execute.toPrettyString()));
        }
    }

    private String fetchJobStatus(Compute compute, String jobName) throws IOException {
        return compute.zoneOperations().get(projectName, ZONE_NAME, jobName).execute().getStatus();
    }

    private void waitForCompletion() {
        LOGGER.info("Waiting for job completion");
        Bucket bucket = getStorage().get(outputBucket);
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

    private Storage getStorage() {
        return StorageOptions.newBuilder().setCredentials(credentials).setProjectId(arguments.project()).build().getService();
    }

    private void stop() throws VmExecutionException {
        LOGGER.info("Stopping [{}]", this);
        try {
            executeSynchronously(initCompute().instances().stop(projectName, ZONE_NAME, vmName));
            LOGGER.info("Stopped [{}]", this);
        } catch (Exception e) {
            String message = format("Failed to stop [%s]", this);
            LOGGER.error(message, e);
            throw new VmExecutionException(message, e);
        }
    }

    @Override
    public String toString() {
        return format("virtual machine %s [%s, %s]", vmName, imageFamily, machineType);
    }
}
