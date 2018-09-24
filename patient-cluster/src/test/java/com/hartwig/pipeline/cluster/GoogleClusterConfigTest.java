package com.hartwig.pipeline.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.bootstrap.NodeInitialization;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

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
                GoogleClusterConfig.from(runtimeBucket, nodeInitialization, PerformanceProfile.builder().workers(5).build());
        assertThat(victim.config().getMasterConfig().getNumInstances()).isEqualTo(1);
        assertThat(victim.config().getWorkerConfig().getNumInstances()).isEqualTo(2);
        assertThat(victim.config().getSecondaryWorkerConfig().getNumInstances()).isEqualTo(3);
    }

    @Test
    public void allNodesUseResolvedMachineType() throws Exception {
        GoogleClusterConfig victim =
                GoogleClusterConfig.from(runtimeBucket, nodeInitialization, PerformanceProfile.builder().cpuPerNode(25).build());
        assertThat(victim.config().getMasterConfig().getMachineTypeUri()).isEqualTo(MachineType.Google.STANDARD_32.uri());
        assertThat(victim.config().getWorkerConfig().getMachineTypeUri()).isEqualTo(MachineType.Google.STANDARD_32.uri());
        assertThat(victim.config().getSecondaryWorkerConfig().getMachineTypeUri()).isEqualTo(MachineType.Google.STANDARD_32.uri());
    }

    @Test
    public void workerDiskSizeSetToValueInProfile() throws Exception {
        int diskSizeGB = 100;
        GoogleClusterConfig victim =
                GoogleClusterConfig.from(runtimeBucket, nodeInitialization, PerformanceProfile.builder().diskSizeGB(diskSizeGB).build());
        assertThat(victim.config().getWorkerConfig().getDiskConfig().getBootDiskSizeGb()).isEqualTo(diskSizeGB);
        assertThat(victim.config().getSecondaryWorkerConfig().getDiskConfig().getBootDiskSizeGb()).isEqualTo(diskSizeGB);
    }

    @Test
    public void yarnVcoreMinimumSetToProfileCpusPerNode() throws Exception {
        int cores = 25;
        GoogleClusterConfig victim =
                GoogleClusterConfig.from(runtimeBucket, nodeInitialization, PerformanceProfile.builder().cpuPerNode(cores).build());
        assertThat(victim.config().getSoftwareConfig().getProperties().get("yarn:yarn.scheduler.minimum-allocation-vcores")).isEqualTo(
                String.valueOf(cores));
    }
}