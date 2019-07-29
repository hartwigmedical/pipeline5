package com.hartwig.pipeline.execution.dataproc;

import static java.lang.String.format;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;

import com.google.api.services.dataproc.v1beta2.model.ClusterConfig;
import com.google.api.services.dataproc.v1beta2.model.DiskConfig;
import com.google.api.services.dataproc.v1beta2.model.GceClusterConfig;
import com.google.api.services.dataproc.v1beta2.model.InstanceGroupConfig;
import com.google.api.services.dataproc.v1beta2.model.LifecycleConfig;
import com.google.api.services.dataproc.v1beta2.model.NodeInitializationAction;
import com.google.api.services.dataproc.v1beta2.model.SoftwareConfig;
import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

class GoogleClusterConfig {

    private static final String IDLE_TTL = "600s";
    private static final int DISK_SIZE_GB = 1000;
    private static final String LATEST_DATAPROC_IMAGE = "1.4";
    private final ClusterConfig config;

    private GoogleClusterConfig(final ClusterConfig config) {
        this.config = config;
    }

    ClusterConfig config() {
        return config;
    }

    static GoogleClusterConfig from(RuntimeBucket runtimeBucket, NodeInitialization nodeInitialization, DataprocPerformanceProfile profile,
            final Arguments arguments) throws FileNotFoundException {
        DiskConfig diskConfig = diskConfig();
        ClusterConfig config = clusterConfig(masterConfig(profile.master()),
                primaryWorkerConfig(diskConfig, profile.primaryWorkers(), profile.numPrimaryWorkers()),
                secondaryWorkerConfig(profile, diskConfig, profile.preemtibleWorkers()),
                runtimeBucket.runId(),
                softwareConfig(),
                initializationActions(runtimeBucket, nodeInitialization),
                gceClusterConfig(arguments),
                lifecycleConfig(IDLE_TTL));
        return new GoogleClusterConfig(config);
    }

    private static GceClusterConfig gceClusterConfig(final Arguments arguments) {
        GceClusterConfig gceClusterConfig = new GceClusterConfig().setMetadata(ImmutableMap.of("bwa_version",
                Versions.BWA,
                "sambamba_version",
                Versions.SAMBAMBA,
                "common_tools_bucket",
                arguments.toolsBucket()));
        arguments.privateNetwork()
                .ifPresent(privateNetwork -> gceClusterConfig.setSubnetworkUri(format("projects/%s/regions/%s/subnetworks/%s",
                        arguments.project(),
                        arguments.region(),
                        privateNetwork)).setInternalIpOnly(true));
        return gceClusterConfig;
    }

    private static LifecycleConfig lifecycleConfig(String idleTtl) {
        return new LifecycleConfig().setIdleDeleteTtl(idleTtl);
    }

    private static SoftwareConfig softwareConfig() {
        return new SoftwareConfig().setImageVersion(LATEST_DATAPROC_IMAGE)
                .setProperties(ImmutableMap.<String, String>builder().put("dataproc:dataproc.logging.stackdriver.enable", "false")
                        .put("yarn:yarn.nodemanager.vmem-check-enabled", "false")
                        .put("yarn:yarn.nodemanager.pmem-check-enabled", "false")
                        .put("dataproc:dataproc.allow.zero.workers", "true")
                        .build());
    }

    @NotNull
    private static List<NodeInitializationAction> initializationActions(final RuntimeBucket runtimeBucket,
            final NodeInitialization nodeInitialization) throws FileNotFoundException {
        return Collections.singletonList(new NodeInitializationAction().setExecutableFile(nodeInitialization.run(runtimeBucket)));
    }

    private static InstanceGroupConfig masterConfig(final MachineType machineType) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(1).setDiskConfig(diskConfig());
    }

    private static ClusterConfig clusterConfig(final InstanceGroupConfig masterConfig, final InstanceGroupConfig primaryWorkerConfig,
            final InstanceGroupConfig secondaryWorkerConfig, final String bucket, final SoftwareConfig softwareConfig,
            final List<NodeInitializationAction> initializationActions, final GceClusterConfig gceClusterConfig,
            final LifecycleConfig lifecycleConfig) {
        return new ClusterConfig().setMasterConfig(masterConfig)
                .setWorkerConfig(primaryWorkerConfig)
                .setSecondaryWorkerConfig(secondaryWorkerConfig)
                .setConfigBucket(bucket)
                .setSoftwareConfig(softwareConfig)
                .setInitializationActions(initializationActions)
                .setGceClusterConfig(gceClusterConfig)
                .setLifecycleConfig(lifecycleConfig);
    }

    private static InstanceGroupConfig primaryWorkerConfig(final DiskConfig diskConfig, final MachineType machineType,
            final int numInstances) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(numInstances).setDiskConfig(diskConfig);
    }

    @NotNull
    private static DiskConfig diskConfig() {
        return new DiskConfig().setBootDiskType("pd-ssd").setBootDiskSizeGb(DISK_SIZE_GB).setNumLocalSsds(2);
    }

    private static InstanceGroupConfig secondaryWorkerConfig(final DataprocPerformanceProfile profile, final DiskConfig diskConfig,
            final MachineType machineType) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri())
                .setNumInstances(profile.numPreemtibleWorkers())
                .setIsPreemptible(true)
                .setDiskConfig(diskConfig);
    }
}
