package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;
import static java.util.Collections.singletonList;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.model.AttachedDisk;
import com.google.api.services.compute.model.AttachedDiskInitializeParams;
import com.google.api.services.compute.model.Image;
import com.google.api.services.compute.model.Instance;
import com.google.api.services.compute.model.Metadata;
import com.google.api.services.compute.model.NetworkInterface;
import com.google.api.services.compute.model.Operation;
import com.google.api.services.compute.model.Scheduling;
import com.google.api.services.compute.model.ServiceAccount;
import com.google.api.services.compute.model.Tags;
import com.google.api.services.compute.model.Zone;
import com.google.api.services.serviceusage.v1beta1.ServiceUsage;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.storage.LocalSsdStorageStrategy;
import com.hartwig.pipeline.execution.vm.storage.PersistentStorageStrategy;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class GoogleComputeEngine implements ComputeEngine {
    private final static String APPLICATION_NAME = "vm-hosted-workload";
    static final String ZONE_EXHAUSTED_ERROR_CODE = "ZONE_RESOURCE_POOL_EXHAUSTED";
    static final String UNSUPPORTED_OPERATION_ERROR_CODE = "UNSUPPORTED_OPERATION";
    static final String QUOTA_EXCEEDED = "QUOTA_EXCEEDED";
    static final String PREEMPTED_INSTANCE = "TERMINATED";
    private static final String LOCAL_SSD = "%s/zones/%s/diskTypes/local-ssd";
    private static final String PD_SSD = "%s/zones/%s/diskTypes/pd-ssd";

    private final Logger LOGGER = LoggerFactory.getLogger(GoogleComputeEngine.class);

    private final CommonArguments arguments;
    private final Compute compute;
    private final Consumer<List<Zone>> zoneRandomizer;
    private final InstanceLifecycleManager lifecycleManager;
    private final BucketCompletionWatcher bucketWatcher;
    private final Labels labels;

    GoogleComputeEngine(final CommonArguments arguments, final Compute compute, final Consumer<List<Zone>> zoneRandomizer,
            final InstanceLifecycleManager lifecycleManager, final BucketCompletionWatcher bucketWatcher, final Labels labels) {
        this.arguments = arguments;
        this.compute = compute;
        this.zoneRandomizer = zoneRandomizer;
        this.lifecycleManager = lifecycleManager;
        this.bucketWatcher = bucketWatcher;
        this.labels = labels;
    }

    public static ComputeEngine from(final CommonArguments arguments, final GoogleCredentials credentials, final Labels labels)
            throws Exception {
        return from(arguments, credentials, true, labels);
    }

    public static ComputeEngine from(final CommonArguments arguments, final GoogleCredentials credentials, final boolean constrainQuotas,
            final Labels labels) throws Exception {
        Compute compute = initCompute(credentials);
        GoogleComputeEngine engine = new GoogleComputeEngine(arguments,
                compute,
                Collections::shuffle,
                new InstanceLifecycleManager(arguments, compute),
                new BucketCompletionWatcher(),
                labels);
        return constrainQuotas ? new QuotaConstrainedComputeEngine(engine,
                initServiceUseage(credentials),
                arguments.region(),
                arguments.project(),
                0.6) : engine;
    }

    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition) {
        return submit(bucket, jobDefinition, "");
    }

    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition, String discriminator) {
        String vmName = format("%s%s-%s", bucket.runId(), discriminator.isEmpty() ? "" : "-" + discriminator, jobDefinition.name());
        RuntimeFiles flags = RuntimeFiles.of(discriminator);
        PipelineStatus status = PipelineStatus.FAILED;
        try {
            BucketCompletionWatcher.State currentState = bucketWatcher.currentState(bucket, flags);
            if (currentState == BucketCompletionWatcher.State.SUCCESS) {
                LOGGER.info("Compute engine job [{}] already exists, and succeeded. Skipping job.", vmName);
                return PipelineStatus.SKIPPED;
            } else if (currentState == BucketCompletionWatcher.State.FAILURE) {
                LOGGER.info("Compute engine job [{}] already exists, but failed. Deleting state and restarting.", vmName);
                bucket.delete(flags.failure());
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
                instance.setTags(new Tags().setItems(arguments.tags()));
                if (arguments.usePreemptibleVms()) {
                    instance.setScheduling(new Scheduling().setPreemptible(true));
                }
                instance.setMachineType(machineType(currentZone.getName(), jobDefinition.performanceProfile().uri(), project));
                final Map<String, String> labelMap = labels.asMap(List.of(Map.entry("job_name", jobDefinition.name())));
                instance.setLabels(labelMap);
                addServiceAccount(instance);
                Image image = attachDisks(compute,
                        instance,
                        jobDefinition,
                        project,
                        vmName,
                        currentZone.getName(),
                        arguments.imageName().isPresent()
                                ? compute.images()
                                .get(arguments.imageProject().orElse(VirtualMachineJobDefinition.HMF_IMAGE_PROJECT),
                                        arguments.imageName().get())
                                .execute()
                                : resolveLatestImage(compute, jobDefinition.imageFamily(), arguments.imageProject().orElse(project)),
                        labelMap);
                LOGGER.info("Submitting compute engine job [{}] using image [{}] in zone [{}]",
                        vmName,
                        image.getName(),
                        currentZone.getName());
                String startupScript = arguments.useLocalSsds()
                        ? jobDefinition.startupCommand()
                        .asUnixString(new LocalSsdStorageStrategy(jobDefinition.localSsdCount()))
                        : jobDefinition.startupCommand().asUnixString(new PersistentStorageStrategy());
                addStartupCommand(instance, bucket, flags, startupScript);
                addNetworkInterface(instance, project);

                Operation result = lifecycleManager.deleteOldInstancesAndStart(instance, currentZone.getName(), vmName);
                if (result.getError() == null) {
                    LOGGER.debug("Successfully initialised [{}]", vmName);
                    status = waitForCompletion(bucket, flags, currentZone, instance);
                    if (status != PipelineStatus.PREEMPTED) {
                        if (arguments.useLocalSsds()) {
                            // Instances with local SSDs cannot be stopped or restarted
                            lifecycleManager.delete(currentZone.getName(), vmName);
                        } else {
                            lifecycleManager.stop(currentZone.getName(), vmName);
                            if (status == PipelineStatus.SUCCESS) {
                                lifecycleManager.delete(currentZone.getName(), vmName);
                            } else {
                                lifecycleManager.disableStartupScript(currentZone.getName(), instance.getName());
                            }
                        }
                        LOGGER.info("Compute engine job [{}] is complete with status [{}]", vmName, status);
                        keepTrying = false;
                    } else {
                        LOGGER.info("Instance [{}] in [{}] was pre-empted", vmName, currentZone.getName());
                    }
                } else if (anyErrorMatch(result, ZONE_EXHAUSTED_ERROR_CODE)) {
                    LOGGER.warn("Zone [{}] has insufficient resources to fulfill the request for [{}]. Trying next zone",
                            currentZone.getName(),
                            vmName);
                } else if (anyErrorMatch(result, UNSUPPORTED_OPERATION_ERROR_CODE)) {
                    LOGGER.warn(
                            "Received unsupported operation from GCE for [{}], this likely means the instance was pre-empted before it could "
                                    + "start, or another operation has yet to complete. Trying next zone.",
                            vmName);
                } else if (anyErrorMatch(result, QUOTA_EXCEEDED)) {
                    throw new RuntimeException(String.format(
                            "Quota exceeded for [%s], will keep trying until resources are available. Quota [%s]",
                            vmName,
                            result.getError().getErrors().get(0).getMessage()));
                } else {
                    throw new RuntimeException(result.getError().toPrettyString());
                }
                index++;
            }
        } catch (IOException e) {
            String message = format("An error occurred running job on compute engine [%s]", vmName);
            LOGGER.error(message, e);
            return PipelineStatus.FAILED;
        }
        return status;
    }

    private static boolean anyErrorMatch(final Operation result, final String errorCode) {
        return result.getError().getErrors().stream().anyMatch(error -> error.getCode().startsWith(errorCode));
    }

    private static Compute initCompute(final GoogleCredentials credentials) throws Exception {
        HttpTransport http = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory json = JacksonFactory.getDefaultInstance();
        return new Compute.Builder(http, json, new HttpCredentialsAdapter(credentials)).setApplicationName(APPLICATION_NAME).build();
    }

    private static ServiceUsage initServiceUseage(final GoogleCredentials credentials) throws Exception {
        HttpTransport http = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory json = JacksonFactory.getDefaultInstance();
        return new ServiceUsage.Builder(http, json, new HttpCredentialsAdapter(credentials)).setApplicationName(APPLICATION_NAME).build();
    }

    private void addNetworkInterface(Instance instance, String projectName) {
        NetworkInterface network = new NetworkInterface();
        network.setNetwork(isUrl(arguments.network())
                ? arguments.network()
                : format("projects/%s/global/networks/%s", projectName, arguments.network()));
        String subnet = arguments.subnet().orElse(arguments.network());
        network.setSubnetwork(isUrl(subnet)
                ? subnet
                : format("projects/%s/regions/%s/subnetworks/%s", projectName, arguments.region(), subnet));
        network.set("no-address", "true");
        instance.setNetworkInterfaces(singletonList(network));
    }

    private boolean isUrl(final String argument) {
        return argument.startsWith("projects");
    }

    private Image attachDisks(final Compute compute, final Instance instance, final VirtualMachineJobDefinition jobDefinition,
            final String projectName, final String vmName, String zone, final Image sourceImage, final Map<String, String> labels)
            throws IOException {
        AttachedDisk bootDisk = new AttachedDisk();
        bootDisk.setBoot(true);
        bootDisk.setAutoDelete(true);
        AttachedDiskInitializeParams bootDiskParams = new AttachedDiskInitializeParams();
        bootDiskParams.setSourceImage(sourceImage.getSelfLink());
        bootDiskParams.setDiskType(pdssd(projectName, zone));
        bootDiskParams.setDiskSizeGb(jobDefinition.baseImageDiskSizeGb());
        bootDiskParams.setLabels(labels);
        bootDisk.setInitializeParams(bootDiskParams);
        List<AttachedDisk> disks = new ArrayList<>(singletonList(bootDisk));
        if (arguments.useLocalSsds()) {
            attachLocalSsds(disks, jobDefinition.localSsdCount(), projectName, zone);
        } else {
            AttachedDiskInitializeParams workingDiskParams = new AttachedDiskInitializeParams();
            workingDiskParams.setDiskType(pdssd(projectName, zone));
            workingDiskParams.setDiskSizeGb(jobDefinition.workingDiskSpaceGb());
            AttachedDisk workingDisk = new AttachedDisk();
            workingDisk.setAutoDelete(true);
            workingDisk.setBoot(false);
            workingDisk.setInitializeParams(workingDiskParams);
            disks.add(workingDisk);
        }
        instance.setDisks(disks);
        compute.instances().attachDisk(projectName, zone, vmName, bootDisk);
        return sourceImage;
    }

    private String pdssd(final String projectName, final String zone) {
        return format(PD_SSD, apiBaseUrl(projectName), zone);
    }

    private void attachLocalSsds(final List<AttachedDisk> disks, final int deviceCount, final String projectName, final String zone) {
        for (int i = 0; i < deviceCount; i++) {
            AttachedDisk disk = new AttachedDisk();
            disk.setBoot(false);
            disk.setAutoDelete(true);
            AttachedDiskInitializeParams params = new AttachedDiskInitializeParams();
            params.setDiskType(format(LOCAL_SSD, apiBaseUrl(projectName), zone));
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

    private void addStartupCommand(Instance instance, RuntimeBucket runtimeBucket, RuntimeFiles runtimeFiles, String startupScript) {
        Metadata startupMetadata = new Metadata();
        Metadata.Items items = new Metadata.Items();
        items.setKey("startup-script");
        items.setValue(startupScript);
        startupMetadata.setItems(singletonList(items));
        runtimeBucket.create(runtimeFiles.startupScript(), startupScript.getBytes());
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

    private PipelineStatus waitForCompletion(RuntimeBucket bucket, RuntimeFiles flags, Zone zone, Instance instance) {
        LOGGER.debug("Waiting for completion of {}", instance.getName());
        return Failsafe.with(new RetryPolicy<>().withMaxRetries(-1).withDelay(Duration.ofSeconds(5)).handleResult(null)).get(() -> {
            LOGGER.debug("Checking bucket for state");
            BucketCompletionWatcher.State bucketState = bucketWatcher.currentState(bucket, flags);
            if (bucketState.equals(BucketCompletionWatcher.State.SUCCESS)) {
                return PipelineStatus.SUCCESS;
            } else if (bucketState.equals(BucketCompletionWatcher.State.FAILURE)) {
                return PipelineStatus.FAILED;
            }
            LOGGER.debug("Bucket does not contain any state yet, checking instance");
            try {
                String status = lifecycleManager.instanceStatus(instance.getName(), zone.getName());
                LOGGER.debug("Execution state of [{}]: [{}]", instance.getName(), status);
                return status.trim().equals(PREEMPTED_INSTANCE) ? PipelineStatus.PREEMPTED : null;
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