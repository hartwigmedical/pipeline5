package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AccessConfig;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.AttachedDiskInitializeParams;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Scheduling;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.api.services.compute.model.Zone;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.CloudExecutor;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.storage.LocalSsdStorageStrategy;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class ComputeEngine implements CloudExecutor<VirtualMachineJobDefinition> {
    public static final int NUMBER_OF_375G_LOCAL_SSD_DEVICES = 4;
    private final static String APPLICATION_NAME = "vm-hosted-workload";
    static final String ZONE_EXHAUSTED_ERROR_CODE = "ZONE_RESOURCE_POOL_EXHAUSTED";
    static final String PREEMPTED_INSTANCE = "TERMINATED";

    private final Logger LOGGER = LoggerFactory.getLogger(ComputeEngine.class);

    private final Arguments arguments;
    private final Compute compute;
    private final Consumer<List<Zone>> zoneRandomizer;
    private final InstanceLifecycleManager lifecycleManager;
    private final BucketCompletionWatcher bucketWatcher;

    public ComputeEngine(final Arguments arguments, final Compute compute, final Consumer<List<Zone>> zoneRandomizer,
            InstanceLifecycleManager lifecycleManager, BucketCompletionWatcher bucketWatcher) {
        this.arguments = arguments;
        this.compute = compute;
        this.zoneRandomizer = zoneRandomizer;
        this.lifecycleManager = lifecycleManager;
        this.bucketWatcher = bucketWatcher;
    }

    public static ComputeEngine from(final Arguments arguments, final GoogleCredentials credentials) throws Exception {
        Compute compute = initCompute(credentials);
        return new ComputeEngine(arguments, compute, Collections::shuffle,
                new InstanceLifecycleManager(arguments, compute),
                new BucketCompletionWatcher());
    }

    @Override
    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition) {
        String vmName = bucket.runId() + "-" + jobDefinition.name();
        PipelineStatus status = PipelineStatus.FAILED;
        try {
            BucketCompletionWatcher.State currentState = bucketWatcher.currentState(bucket, jobDefinition);
            if (currentState == BucketCompletionWatcher.State.SUCCESS) {
                LOGGER.info("Compute engine job [{}] already exists, and succeeded. Skipping job.", vmName);
                return PipelineStatus.SKIPPED;
            } else if (currentState == BucketCompletionWatcher.State.FAILURE) {
                LOGGER.info("Compute engine job [{}] already exists, but failed. Deleting state and restarting.", vmName);
                bucket.delete(jobDefinition.startupCommand().failureFlag());
                bucket.delete(jobDefinition.namespacedResults().path());
            }

            String project = arguments.project();
            List<Zone> zones = fetchZones();
            zoneRandomizer.accept(zones);
            int index = 0;
            boolean keepTrying = !zones.isEmpty();
            while (keepTrying) {
                Zone currentZone = zones.get(index % zones.size());
                Instance instance = lifecycleManager.newInstance();
                instance.setName(vmName);
                instance.setZone(currentZone.getName());
                if (arguments.usePreemptibleVms() && jobDefinition.preemptible()) {
                    instance.setScheduling(new Scheduling().setPreemptible(true));
                }
                instance.setMachineType(machineType(currentZone.getName(), jobDefinition.performanceProfile().uri(), project));

                instance.setLabels(Labels.ofRun(bucket.runId(), jobDefinition.name(), arguments));

                addServiceAccount(instance);
                Image image = attachDisks(compute,
                        instance,
                        jobDefinition.imageFamily(),
                        project,
                        vmName,
                        currentZone.getName());
                LOGGER.info("Submitting compute engine job [{}] using image [{}] in zone [{}]",
                        vmName,
                        image.getName(),
                        currentZone.getName());
                String startupScript = arguments.useLocalSsds()
                        ? jobDefinition.startupCommand().asUnixString(new LocalSsdStorageStrategy(NUMBER_OF_375G_LOCAL_SSD_DEVICES))
                        : jobDefinition.startupCommand().asUnixString();
                addStartupCommand(instance, bucket, startupScript);
                addNetworkInterface(instance, project);

                Operation result = lifecycleManager.deleteOldInstancesAndStart(instance, currentZone.getName(), vmName);
                if (result.getError() == null) {
                    LOGGER.debug("Successfully initialised [{}]", vmName);
                    status = waitForCompletion(bucket, jobDefinition, currentZone, instance);
                    if (status != PipelineStatus.PREEMPTED) {
                        lifecycleManager.stop(currentZone.getName(), vmName);
                        if (status == PipelineStatus.SUCCESS) {
                            lifecycleManager.delete(currentZone.getName(), vmName);
                        } else {
                            lifecycleManager.disableStartupScript(currentZone.getName(), instance.getName());
                        }
                        LOGGER.info("Compute engine job [{}] is complete with status [{}]", vmName, status);
                        keepTrying = false;
                    } else {
                        LOGGER.info("Instance [{}] in [{}] was pre-empted", vmName, currentZone.getName());
                    }
                } else if (result.getError().getErrors().stream().anyMatch(error -> error.getCode().startsWith(ZONE_EXHAUSTED_ERROR_CODE))) {
                    LOGGER.warn("Zone [{}] has insufficient resources to fulfill the request for [{}]. Trying next zone",
                            currentZone.getName(),
                            vmName);
                } else {
                    throw new RuntimeException(result.getError().toPrettyString());
                }
                index++;
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

    private Image attachDisks(Compute compute, Instance instance, String imageFamily, String projectName, String vmName,
            String zone) throws IOException {
        Image sourceImage = resolveLatestImage(compute, imageFamily, projectName);
        AttachedDisk disk = new AttachedDisk();
        disk.setBoot(true);
        disk.setAutoDelete(true);
        AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
        params.setSourceImage(sourceImage.getSelfLink());
        params.setDiskType(format("%s/zones/%s/diskTypes/pd-ssd", apiBaseUrl(projectName), zone));
        params.setDiskSizeGb(arguments.useLocalSsds() ? 10L : 1000L);
        disk.setInitializeParams(params);
        List<AttachedDisk> disks = new ArrayList<>(asList(disk));
        if (arguments.useLocalSsds()) {
            attachLocalSsds(disks, projectName, zone);
        }
        instance.setDisks(disks);
        compute.instances().attachDisk(projectName, zone, vmName, disk);
        return sourceImage;
    }

    private void attachLocalSsds(List<AttachedDisk> disks, String projectName, String zone) {
        for (int i = 0; i < NUMBER_OF_375G_LOCAL_SSD_DEVICES; i++) {
            AttachedDisk disk = new AttachedDisk();
            disk.setBoot(false);
            disk.setAutoDelete(true);
            AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
            params.setDiskType(format("%s/zones/%s/diskTypes/local-ssd", apiBaseUrl(projectName), zone));
            disk.setInitializeParams(params);
            disk.setType("SCRATCH");
            disk.setInterface("NVME");
            disks.add(disk);
        }
    }

    private void addServiceAccount(Instance instance) {
        ServiceAccount account = new ServiceAccount();
        account.setEmail(arguments.serviceAccountEmail());
        account.setScopes(singletonList("https://www.googleapis.com/auth/cloud-platform"));
        instance.setServiceAccounts(singletonList(account));
    }

    private void addStartupCommand(Instance instance, RuntimeBucket runtimeBucket, String startupScript) {
        Metadata startupMetadata = new Metadata();
        Metadata.Items items = new Metadata.Items();
        items.setKey("startup-script");
        items.setValue(startupScript);
        startupMetadata.setItems(singletonList(items));
        runtimeBucket.create("copy_of_startup_script_for_run.sh", startupScript.getBytes());
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

    private PipelineStatus waitForCompletion(RuntimeBucket bucket, VirtualMachineJobDefinition jobDefinition, Zone zone,
            Instance instance) {
        LOGGER.debug("Waiting for completion of {}", instance.getName());
        return Failsafe.with(new RetryPolicy<>().withMaxRetries(-1).withDelay(Duration.ofSeconds(5)).handleResult(null)).get(() -> {
            LOGGER.debug("Checking bucket for state");
            BucketCompletionWatcher.State bucketState = bucketWatcher.currentState(bucket, jobDefinition);
            if (bucketState.equals(BucketCompletionWatcher.State.SUCCESS)) {
                return PipelineStatus.SUCCESS;
            } else if (bucketState.equals(BucketCompletionWatcher.State.FAILURE)) {
                return PipelineStatus.FAILED;
            }
            LOGGER.debug("Bucket does not contain any state yet, checking instance");
            try {
                String status = lifecycleManager.instanceStatus(instance.getName(), zone.getName());
                LOGGER.debug("Execution state of [{}]: [{}]", instance.getName(), status);
                switch (status.trim()) {
                    case PREEMPTED_INSTANCE: return PipelineStatus.PREEMPTED;
                    default: return null;
                }
            } catch (Exception e) {
                LOGGER.debug("Caught exception when looking for operationStatus, will continue to wait", e);
                return null;
            }
        });
    }

    private List<Zone> fetchZones() throws IOException {
        return compute.zones()
                .list(arguments.project())
                .execute()
                .getItems()
                .stream()
                .filter(zone -> zone.getRegion().endsWith(arguments.region()))
                .collect(Collectors.toList());
    }
}
