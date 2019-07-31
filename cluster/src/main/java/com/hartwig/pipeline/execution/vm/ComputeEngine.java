package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
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
import com.google.api.services.compute.model.Zone;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.CloudExecutor;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;

public class ComputeEngine implements CloudExecutor<VirtualMachineJobDefinition> {
    private final static String APPLICATION_NAME = "vm-hosted-workload";

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
    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition) {
        String vmName = bucket.runId() + "-" + jobDefinition.name();
        PipelineStatus status = PipelineStatus.FAILED;
        try {
            if (bucketContainsFile(bucket, jobDefinition.startupCommand().successFlag())) {
                LOGGER.info("Compute engine job [{}] already exists, and succeeded. Skipping job.", vmName);
                return PipelineStatus.SKIPPED;
            } else if (bucketContainsFile(bucket, jobDefinition.startupCommand().failureFlag())) {
                LOGGER.info("Compute engine job [{}] already exists, but failed. Deleting state and restarting.", vmName);
                bucket.delete(jobDefinition.startupCommand().failureFlag());
                bucket.delete(jobDefinition.namespacedResults().path());
            }

            List<Zone> zones = compute.zones()
                    .list(arguments.project())
                    .execute()
                    .getItems()
                    .stream()
                    .filter(zone -> zone.getRegion().endsWith(arguments.region()))
                    .collect(Collectors.toList());
            for (Zone zone : zones) {
                Instance instance = new Instance();
                instance.setName(vmName);
                instance.setZone(zone.getName());
                String project = arguments.project();
                instance.setMachineType(machineType(zone.getName(), jobDefinition.performanceProfile().uri(), project));

                instance.setLabels(ImmutableMap.of("run_id", bucket.runId(), "job_name", jobDefinition.name()));

                addServiceAccount(instance);
                Image image = attachDisk(compute,
                        instance,
                        jobDefinition.imageFamily(),
                        project,
                        vmName,
                        jobDefinition.performanceProfile().diskGb(),
                        zone.getName());
                LOGGER.info("Submitting compute engine job [{}] using image [{}] in zone [{}]",
                        jobDefinition.name(),
                        image.getName(),
                        zone.getName());
                addStartupCommand(instance, bucket, jobDefinition.startupCommand());
                addNetworkInterface(instance, project);

                Operation result = deleteOldInstancesAndStart(compute, instance, project, zone.getName(), vmName);
                if (result.getError() == null) {
                    LOGGER.debug("Successfully initialised [{}]", vmName);
                    status = waitForCompletion(bucket, jobDefinition);
                    stop(project, zone.getName(), vmName);
                    if (status == PipelineStatus.SUCCESS) {
                        delete(project, zone.getName(), vmName);
                    }
                    LOGGER.info("Compute engine job [{}] is complete with status [{}]", jobDefinition.name(), status);
                    break;
                } else if (result.getError()
                        .getErrors()
                        .stream()
                        .anyMatch(error -> error.getCode().equals("ZONE_RESOURCE_POOL_EXHAUSTED"))) {
                    LOGGER.warn("Zone [{}] has insufficient resources to fulfill the request. Trying next zone", zone.getName());
                } else {
                    throw new RuntimeException(result.getError().toPrettyString());
                }
            }
        } catch (Exception e) {
            String message = format("An error occurred running job on compute engine [%s]", vmName);
            LOGGER.error(message, e);
            return PipelineStatus.FAILED;
        }
        return status;
    }

    private static Compute initCompute(final GoogleCredentials credentials) throws Exception {
        HttpTransport http = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory json = JacksonFactory.getDefaultInstance();
        return new Compute.Builder(http, json, new HttpCredentialsAdapter(credentials)).setApplicationName(APPLICATION_NAME).build();
    }

    private void addNetworkInterface(Instance instance, String projectName) {
        NetworkInterface networkInterface = arguments.privateNetwork().map(network -> {
            NetworkInterface privateNetwork = new NetworkInterface();
            privateNetwork.setNetwork(format("%s/global/networks/%s", apiBaseUrl(projectName), network));
            privateNetwork.setSubnetwork(format("%s/regions/%s/subnetworks/%s", apiBaseUrl(projectName), arguments.region(), network));
            privateNetwork.set("no-address", "true");
            return privateNetwork;
        }).orElseGet(() -> {
            NetworkInterface publicNetwork = new NetworkInterface();
            AccessConfig config = new AccessConfig();
            publicNetwork.setNetwork(format("%s/global/networks/default", apiBaseUrl(projectName)));
            config.setType("ONE_TO_ONE_NAT");
            config.setName("External NAT");
            publicNetwork.setAccessConfigs(singletonList(config));
            return publicNetwork;
        });
        instance.setNetworkInterfaces(singletonList(networkInterface));
    }

    private Image attachDisk(Compute compute, Instance instance, String imageFamily, String projectName, String vmName, long diskSizeGB,
            String zone) throws IOException {
        Image sourceImage = resolveLatestImage(compute, imageFamily, projectName);
        AttachedDisk disk = new AttachedDisk();
        disk.setBoot(true);
        disk.setAutoDelete(true);
        AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
        params.setDiskSizeGb(diskSizeGB);
        params.setSourceImage(sourceImage.getSelfLink());
        params.setDiskType(format("%s/zones/%s/diskTypes/pd-ssd", apiBaseUrl(projectName), zone));
        disk.setInitializeParams(params);
        instance.setDisks(singletonList(disk));
        compute.instances().attachDisk(projectName, zone, vmName, disk);
        return sourceImage;
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
        runtimeBucket.create("copy_of_startup_script_used_for_this_run.sh", startupCommand.asUnixString().getBytes());
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
    private Operation deleteOldInstancesAndStart(Compute compute, Instance instance, String projectName, String zoneName, String vmName)
            throws Exception {
        Compute.Instances.Insert insert = compute.instances().insert(projectName, zoneName, instance);
        try {
            return executeSynchronously(insert, projectName, zoneName);
        } catch (Exception e) {
            if (e.getCause() instanceof GoogleJsonResponseException) {
                GoogleJsonResponseException gjre = (GoogleJsonResponseException) e.getCause();
                if (HttpURLConnection.HTTP_CONFLICT == gjre.getDetails().getCode()) {
                    LOGGER.info("Found existing [{}] instance; deleting and restarting", vmName);
                    executeSynchronously(compute.instances().delete(projectName, zoneName, vmName), projectName, zoneName);
                    executeSynchronously(insert, projectName, zoneName);
                } else {
                    throw gjre;
                }
            } else {
                throw e;
            }
        }
        return new Operation();
    }

    private Operation executeSynchronously(ComputeRequest<Operation> request, String projectName, String zoneName) throws Exception {
        Operation syncOp = executeWithRetries(request::execute);
        String logId = format("Operation [%s:%s]", syncOp.getOperationType(), syncOp.getName());
        LOGGER.debug("{} is executing synchronously", logId);
        while ("RUNNING".equals(fetchJobStatus(compute, syncOp.getName(), projectName, zoneName))) {
            LOGGER.debug("{} not done yet", logId);
            try {
                Thread.sleep(500);
            } catch (InterruptedException ie) {
                Thread.interrupted();
            }
        }

        return executeWithRetries(() -> compute.zoneOperations().get(projectName, zoneName, syncOp.getName()).execute());
    }

    private String fetchJobStatus(Compute compute, String jobName, String projectName, String zoneName) {
        return executeWithRetries(() -> compute.zoneOperations().get(projectName, zoneName, jobName).execute()).getStatus();
    }

    private Operation executeWithRetries(final CheckedSupplier<Operation> operationCheckedSupplier) {
        return Failsafe.with(new RetryPolicy<>().handle(IOException.class).withDelay(Duration.ofSeconds(5)).withMaxRetries(5))
                .get(operationCheckedSupplier);
    }

    private boolean bucketContainsFile(RuntimeBucket bucket, String filename) {
        List<Blob> objects = bucket.list();
        for (Blob blob : objects) {
            String name = blob.getName();
            if (name.equals(bucket.getNamespace() + "/" + filename)) {
                return true;
            }
        }
        return false;
    }

    private PipelineStatus waitForCompletion(RuntimeBucket bucket, VirtualMachineJobDefinition jobDefinition) {
        LOGGER.debug("Waiting for job completion");
        while (true) {
            try {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                } catch (InterruptedException ie) {
                    Thread.interrupted();
                }
                if (bucketContainsFile(bucket, jobDefinition.startupCommand().successFlag())) {
                    return PipelineStatus.SUCCESS;
                } else if (bucketContainsFile(bucket, jobDefinition.startupCommand().failureFlag())) {
                    return PipelineStatus.FAILED;
                }
            } catch (Exception e) {
                LOGGER.error("Error while polling the results bucket for completion flag. Will try again in [5] seconds", e);
            }
        }
    }

    private void stop(String projectName, String zoneName, String vmName) throws Exception {
        executeSynchronously(compute.instances().stop(projectName, zoneName, vmName), projectName, zoneName);
    }

    private void delete(String projectName, String zoneName, String vmName) throws Exception {
        executeSynchronously(compute.instances().delete(projectName, zoneName, vmName), projectName, zoneName);
    }
}
