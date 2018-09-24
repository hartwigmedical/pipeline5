package com.hartwig.pipeline.cluster;

import java.io.FileNotFoundException;
import java.util.Collections;

import com.google.api.services.dataproc.model.ClusterConfig;
import com.google.api.services.dataproc.model.DiskConfig;
import com.google.api.services.dataproc.model.InstanceGroupConfig;
import com.google.api.services.dataproc.model.NodeInitializationAction;
import com.google.api.services.dataproc.model.SoftwareConfig;
import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.bootstrap.NodeInitialization;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

import org.jetbrains.annotations.NotNull;

class GoogleClusterConfig {

    private final ClusterConfig config;

    private GoogleClusterConfig(final ClusterConfig config) {
        this.config = config;
    }

    ClusterConfig config() {
        return config;
    }

    static GoogleClusterConfig from(RuntimeBucket runtimeBucket, NodeInitialization nodeInitialization, PerformanceProfile profile)
            throws FileNotFoundException {
        DiskConfig diskConfig = diskConfig(profile);
        MachineType machineType = MachineType.from(profile);
        ClusterConfig config = clusterConfig(masterConfig(machineType),
                primaryWorkerConfig(diskConfig, machineType),
                secondaryWorkerConfig(profile, diskConfig, machineType),
                runtimeBucket.getName(),
                nodeInitialization.run(runtimeBucket),
                profile);
        return new GoogleClusterConfig(config);
    }

    private static InstanceGroupConfig masterConfig(final MachineType machineType) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(1);
    }

    private static ClusterConfig clusterConfig(final InstanceGroupConfig masterConfig, final InstanceGroupConfig primaryWorkerConfig,
            final InstanceGroupConfig secondaryWorkerConfig, final String bucket, final String nodeExecutableLocation,
            final PerformanceProfile profile) {
        return new ClusterConfig().setMasterConfig(masterConfig)
                .setWorkerConfig(primaryWorkerConfig)
                .setSecondaryWorkerConfig(secondaryWorkerConfig)
                .setConfigBucket(bucket)
                .setSoftwareConfig(new SoftwareConfig().setProperties(ImmutableMap.<String, String>builder().put(
                        "yarn:yarn.scheduler.minimum-allocation-vcores",
                        String.valueOf(profile.cpuPerNode()))
                        .put("capacity-scheduler:yarn.scheduler.capacity.resource-calculator",
                                "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator")
                        .build()))
                .setInitializationActions(Collections.singletonList(new NodeInitializationAction().setExecutableFile(nodeExecutableLocation)));
    }

    private static InstanceGroupConfig primaryWorkerConfig(final DiskConfig diskConfig, final MachineType machineType) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri()).setNumInstances(2).setDiskConfig(diskConfig);
    }

    @NotNull
    private static DiskConfig diskConfig(PerformanceProfile profile) {
        return new DiskConfig().setBootDiskSizeGb(profile.diskSizeGB());
    }

    private static InstanceGroupConfig secondaryWorkerConfig(final PerformanceProfile profile, final DiskConfig diskConfig,
            final MachineType machineType) {
        return new InstanceGroupConfig().setMachineTypeUri(machineType.uri())
                .setNumInstances(profile.workers() - 2)
                .setIsPreemptible(true)
                .setDiskConfig(diskConfig);
    }
}
