package com.hartwig.pipeline.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.bootstrap.NodeInitialization;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;
import com.hartwig.pipeline.performance.ImmutablePerformanceProfile;
import com.hartwig.pipeline.performance.MachineType;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class GoogleClusterConfigTest {

    private RuntimeBucket runtimeBucket;
    private NodeInitialization nodeInitialization;

    @Before
    public void setUp() throws Exception {
        runtimeBucket = mock(RuntimeBucket.class);
        when(runtimeBucket.getName()).thenReturn("runtime-bucket");
        nodeInitialization = mock(NodeInitialization.class);
    }

    @Test
    public void oneMasterTwoPrimaryWorkersAndRemainingNodesSecondary() throws Exception {
        GoogleClusterConfig victim =
                GoogleClusterConfig.from(runtimeBucket, nodeInitialization, profileBuilder().numPreemtibleWorkers(3).build());
        assertThat(victim.config().getMasterConfig().getNumInstances()).isEqualTo(1);
        assertThat(victim.config().getWorkerConfig().getNumInstances()).isEqualTo(2);
        assertThat(victim.config().getSecondaryWorkerConfig().getNumInstances()).isEqualTo(3);
    }

    @Test
    public void allNodesUseResolvedMachineType() throws Exception {
        GoogleClusterConfig victim = GoogleClusterConfig.from(runtimeBucket, nodeInitialization, profileBuilder().build());
        assertThat(victim.config().getMasterConfig().getMachineTypeUri()).isEqualTo(MachineType.GOOGLE_STANDARD_16);
        assertThat(victim.config().getWorkerConfig().getMachineTypeUri()).isEqualTo(MachineType.GOOGLE_STANDARD_32);
        assertThat(victim.config().getSecondaryWorkerConfig().getMachineTypeUri()).isEqualTo(MachineType.GOOGLE_STANDARD_32);
    }

    @Test
    public void workerDiskSizeSetToValueInProfile() throws Exception {
        GoogleClusterConfig victim = GoogleClusterConfig.from(runtimeBucket, nodeInitialization, profileBuilder().build());
        assertThat(victim.config().getWorkerConfig().getDiskConfig().getBootDiskSizeGb()).isEqualTo(MachineType.DISK_GB);
        assertThat(victim.config().getSecondaryWorkerConfig().getDiskConfig().getBootDiskSizeGb()).isEqualTo(MachineType.DISK_GB);
    }

    @Test
    public void yarnVcoreMinimumSetToProfileCpusPerNode() throws Exception {
        GoogleClusterConfig victim = GoogleClusterConfig.from(runtimeBucket, nodeInitialization, profileBuilder().build());
        assertThat(victim.config().getSoftwareConfig().getProperties().get("yarn:yarn.scheduler.minimum-allocation-vcores")).isEqualTo(
                String.valueOf(MachineType.defaultWorker().cpus()));
    }

    @NotNull
    private ImmutablePerformanceProfile.Builder profileBuilder() {
        return PerformanceProfile.builder().numPreemtibleWorkers(5).numPrimaryWorkers(2);
    }
}