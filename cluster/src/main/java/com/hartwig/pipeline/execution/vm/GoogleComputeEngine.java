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
import java.util.stream.StreamSupport;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.serviceusage.v1beta1.ServiceUsage;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.compute.v1.AttachedDisk;
import com.google.cloud.compute.v1.AttachedDiskInitializeParams;
import com.google.cloud.compute.v1.Image;
import com.google.cloud.compute.v1.ImagesClient;
import com.google.cloud.compute.v1.ImagesSettings;
import com.google.cloud.compute.v1.Instance;
import com.google.cloud.compute.v1.InstancesClient;
import com.google.cloud.compute.v1.InstancesSettings;
import com.google.cloud.compute.v1.Items;
import com.google.cloud.compute.v1.Metadata;
import com.google.cloud.compute.v1.NetworkInterface;
import com.google.cloud.compute.v1.Operation;
import com.google.cloud.compute.v1.Scheduling;
import com.google.cloud.compute.v1.ServiceAccount;
import com.google.cloud.compute.v1.Tags;
import com.google.cloud.compute.v1.Zone;
import com.google.cloud.compute.v1.ZoneOperationsClient;
import com.google.cloud.compute.v1.ZoneOperationsSettings;
import com.google.cloud.compute.v1.ZonesClient;
import com.google.cloud.compute.v1.ZonesSettings;
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
    private final ZonesClient zonesClient;
    private final ImagesClient images;
    private final Consumer<List<Zone>> zoneRandomizer;
    private final InstanceLifecycleManager lifecycleManager;
    private final BucketCompletionWatcher bucketWatcher;
    private final Labels labels;

    GoogleComputeEngine(final CommonArguments arguments, final ZonesClient zonesClient, final ImagesClient images,
            final Consumer<List<Zone>> zoneRandomizer, final InstanceLifecycleManager lifecycleManager,
            final BucketCompletionWatcher bucketWatcher, final Labels labels) {
        this.arguments = arguments;
        this.zonesClient = zonesClient;
        this.images = images;
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
        final InstancesClient instances =
                InstancesClient.create(InstancesSettings.newBuilder().setCredentialsProvider(() -> credentials).build());
        GoogleComputeEngine engine = new GoogleComputeEngine(arguments,
                ZonesClient.create(ZonesSettings.newBuilder().setCredentialsProvider(() -> credentials).build()),
                ImagesClient.create(ImagesSettings.newBuilder().setCredentialsProvider(() -> credentials).build()),
                Collections::shuffle,
                new InstanceLifecycleManager(arguments,
                        instances,
                        ZonesClient.create(ZonesSettings.newBuilder().setCredentialsProvider(() -> credentials).build()),
                        ZoneOperationsClient.create(ZoneOperationsSettings.newBuilder().setCredentialsProvider(() -> credentials).build())),
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

    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition, final String discriminator) {
        String vmName = format("%s%s-%s", bucket.runId(), discriminator.isEmpty() ? "" : "-" + discriminator, jobDefinition.name());
        RuntimeFiles flags = RuntimeFiles.of(discriminator);
        PipelineStatus status = PipelineStatus.FAILED;
        try {
            BucketCompletionWatcher.State currentState = bucketWatcher.currentState(bucket, flags);
            if (currentState == BucketCompletionWatcher.State.SUCCESS) {
                LOGGER.info("Compute engine job [{}] already exists, and succeeded. Skipping job.", vmName);
                lifecycleManager.findExistingInstance(vmName).ifPresent(instance -> {
                    String zone = instance.getZone();
                    LOGGER.info("Deleting leftover [{}] instance after successful run", vmName);
                    lifecycleManager.delete(vmName, zone);
                });
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
                final Tags.Builder tagsBuilder = Tags.newBuilder();
                arguments.tags().forEach(tagsBuilder::addItems);
                final Map<String, String> labelMap = labels.asMap(List.of(Map.entry("job_name", jobDefinition.name())));
                Instance.Builder instanceBuilder = Instance.newBuilder()
                        .setName(vmName)
                        .setZone(currentZone.getName())
                        .setTags(tagsBuilder.build())
                        .setMachineType(machineType(currentZone.getName(), jobDefinition.performanceProfile().uri(), project))
                        .putAllLabels(labelMap);

                if (arguments.usePreemptibleVms()) {
                    final Scheduling.Builder schedulingBuilder = Scheduling.newBuilder().setProvisioningModel("SPOT");
                    instanceBuilder.setScheduling(schedulingBuilder.build());
                }

                addServiceAccount(instanceBuilder);
                Image image = attachDisks(instanceBuilder,
                        jobDefinition,
                        project,
                        currentZone.getName(),
                        arguments.imageName().isPresent()
                                ? images.get(arguments.imageProject()
                                .orElse(VirtualMachineJobDefinition.HMF_IMAGE_PROJECT), arguments.imageName().get())
                                : resolveLatestImage(images, jobDefinition.imageFamily(), arguments.imageProject().orElse(project)),
                        labelMap);
                LOGGER.info("Submitting compute engine job [{}] using image [{}] in zone [{}]",
                        vmName,
                        image.getName(),
                        currentZone.getName());
                String startupScript = arguments.useLocalSsds()
                        ? jobDefinition.startupCommand()
                        .asUnixString(new LocalSsdStorageStrategy(jobDefinition.localSsdCount()))
                        : jobDefinition.startupCommand().asUnixString(new PersistentStorageStrategy());
                addStartupCommand(instanceBuilder, bucket, flags, startupScript);
                addNetworkInterface(instanceBuilder, project);

                Instance instance = instanceBuilder.build();
                Operation result = lifecycleManager.deleteOldInstancesAndStart(instance, currentZone.getName(), vmName);
                if (result.getError().getErrorsList().isEmpty()) {
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
                            result.getError().getErrorsList().get(0).getMessage()));
                } else {
                    throw new RuntimeException(result.getError().getErrorsList().get(0).getMessage());
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
        return result.getError().getErrorsList().stream().anyMatch(error -> error.getCode().startsWith(errorCode));
    }

    private static ServiceUsage initServiceUseage(final GoogleCredentials credentials) throws Exception {
        HttpTransport http = GoogleNetHttpTransport.newTrustedTransport();
        JsonFactory json = GsonFactory.getDefaultInstance();
        return new ServiceUsage.Builder(http, json, new HttpCredentialsAdapter(credentials)).setApplicationName(APPLICATION_NAME).build();
    }

    private void addNetworkInterface(final Instance.Builder instanceBuilder, final String projectName) {
        NetworkInterface.Builder networkBuilder = NetworkInterface.newBuilder()
                .setNetwork(isUrl(arguments.network())
                        ? arguments.network()
                        : format("projects/%s/global/networks/%s", projectName, arguments.network()));
        String subnet = arguments.subnet().orElse(arguments.network());
        networkBuilder.setSubnetwork(isUrl(subnet)
                ? subnet
                : format("projects/%s/regions/%s/subnetworks/%s", projectName, arguments.region(), subnet));

        instanceBuilder.addNetworkInterfaces(networkBuilder.build());
    }

    private boolean isUrl(final String argument) {
        return argument.startsWith("projects");
    }

    private Image attachDisks(final Instance.Builder instanceBuilder, final VirtualMachineJobDefinition jobDefinition,
            final String projectName, final String zone, final Image sourceImage, final Map<String, String> labels) throws IOException {
        AttachedDisk bootDisk = AttachedDisk.newBuilder()
                .setBoot(true)
                .setAutoDelete(true)
                .setInitializeParams(AttachedDiskInitializeParams.newBuilder()
                        .setSourceImage(sourceImage.getSelfLink())
                        .setDiskType(pdssd(projectName, zone))
                        .setDiskSizeGb(jobDefinition.baseImageDiskSizeGb())
                        .putAllLabels(labels)
                        .build())
                .build();
        List<AttachedDisk> disks = new ArrayList<>(singletonList(bootDisk));
        if (arguments.useLocalSsds()) {
            attachLocalSsds(disks, jobDefinition.localSsdCount(), projectName, zone);
        } else {
            disks.add(AttachedDisk.newBuilder()
                    .setAutoDelete(true)
                    .setBoot(false)
                    .setInitializeParams(AttachedDiskInitializeParams.newBuilder()
                            .setDiskType(pdssd(projectName, zone))
                            .setDiskSizeGb(jobDefinition.workingDiskSpaceGb())
                            .build())
                    .build());
        }
        instanceBuilder.addAllDisks(disks);
        return sourceImage;
    }

    private String pdssd(final String projectName, final String zone) {
        return format(PD_SSD, apiBaseUrl(projectName), zone);
    }

    private void attachLocalSsds(final List<AttachedDisk> disks, final int deviceCount, final String projectName, final String zone) {
        for (int i = 0; i < deviceCount; i++) {
            disks.add(AttachedDisk.newBuilder()
                    .setBoot(false)
                    .setAutoDelete(true)
                    .setType("SCRATCH")
                    .setInterface("NVME")
                    .setInitializeParams(AttachedDiskInitializeParams.newBuilder()
                            .setDiskType(format(LOCAL_SSD, apiBaseUrl(projectName), zone))
                            .build())
                    .build());
        }
    }

    private void addServiceAccount(final Instance.Builder instance) {
        instance.addServiceAccounts(ServiceAccount.newBuilder()
                .setEmail(arguments.serviceAccountEmail())
                .addScopes("https://www.googleapis.com/auth/cloud-platform")
                .build());
    }

    private void addStartupCommand(final Instance.Builder instance, final RuntimeBucket runtimeBucket, final RuntimeFiles runtimeFiles,
            final String startupScript) {
        Metadata startupMetadata =
                Metadata.newBuilder().addItems(Items.newBuilder().setKey("startup-script").setValue(startupScript)).build();
        runtimeBucket.create(runtimeFiles.startupScript(), startupScript.getBytes());
        instance.setMetadata(startupMetadata);
    }

    private Image resolveLatestImage(final ImagesClient images, final String sourceImageFamily, final String projectName)
            throws IOException {
        Image image = images.getFromFamily(projectName, sourceImageFamily);
        if (image != null) {
            return image;
        }
        throw new IllegalArgumentException(format("No image for family [%s]", sourceImageFamily));
    }

    private String apiBaseUrl(final String projectName) {
        return format("https://www.googleapis.com/compute/v1/projects/%s", projectName);
    }

    private String machineType(final String zone, final String type, final String projectName) {
        return format("%s/zones/%s/machineTypes/%s", apiBaseUrl(projectName), zone, type);
    }

    private PipelineStatus waitForCompletion(final RuntimeBucket bucket, final RuntimeFiles flags, final Zone zone,
            final Instance instance) {
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
        return StreamSupport.stream(zonesClient.list(arguments.project()).iterateAll().spliterator(), false)
                .filter(zone -> zone.getRegion().endsWith(arguments.region()))
                .collect(Collectors.toList());
    }
}