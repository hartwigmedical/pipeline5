package com.hartwig.pipeline.execution.dataproc;

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
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.performance.MachineType;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.jetbrains.annotations.NotNull;

class GoogleClusterConfig {

    private static final String IDLE_TTL = "600s";
    private final ClusterConfig config;

    private GoogleClusterConfig(final ClusterConfig config) {
        this.config = config;
    }

    ClusterConfig config() {
        return config;
    }

    static GoogleClusterConfig from(RuntimeBucket runtimeBucket, NodeInitialization nodeInitialization, PerformanceProfile profile)
            throws FileNotFoundException {
        DiskConfig diskConfig = diskConfig(profile.primaryWorkers());
        ClusterConfig config = clusterConfig(masterConfig(profile.master()),
                primaryWorkerConfig(diskConfig, profile.primaryWorkers(), profile.numPrimaryWorkers()),
                secondaryWorkerConfig(profile, diskConfig, profile.preemtibleWorkers()),
                runtimeBucket.name(),
                softwareConfig(),
                initializationActions(runtimeBucket, nodeInitialization), gceClusterConfig(), lifecycleConfig(IDLE_TTL));
        return new GoogleClusterConfig(config);
    }

    private static GceClusterConfig gceClusterConfig() {
        return new GceClusterConfig();
    }

    private static LifecycleConfig lifecycleConfig(String idleTtl) {
        return new LifecycleConfig().setIdleDeleteTtl(idleTtl);
    }

    private static SoftwareConfig softwareConfig() {
        return new SoftwareConfig().setProperties(ImmutableMap.<String, String>builder().put("dataproc:dataproc.logging.stackdriver.enable",
                "false")
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
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(1).setDiskConfig(diskConfig(machineType));
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
    private static DiskConfig diskConfig(MachineType machineType) {
        return new DiskConfig().setBootDiskSizeGb(machineType.diskGB());
    }

    private static InstanceGroupConfig secondaryWorkerConfig(final PerformanceProfile profile, final DiskConfig diskConfig,
            final MachineType machineType) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(profile.numPreemtibleWorkers())
                .setIsPreemptible(true)
                .setDiskConfig(diskConfig);
    }
}
