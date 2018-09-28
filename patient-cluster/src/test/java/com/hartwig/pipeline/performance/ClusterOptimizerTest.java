package com.hartwig.pipeline.performance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;

import org.junit.Before;
import org.junit.Test;

public class ClusterOptimizerTest {

    private static final ImmutableSample SAMPLE_WITH_TWO_LANES =
            Sample.builder("test", "test").addLanes(mock(Lane.class), mock(Lane.class)).build();
    private ClusterOptimizer victim;

    @Before
    public void setUp() throws Exception {
        victim = new ClusterOptimizer(CpuFastQSizeRatio.of(5), filename -> 10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void noLanesInSampleThrowsIllegalArgument() {
        victim.optimize(Sample.builder("test", "test").build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyFilesThrowsIllegalArgumentException() {
        victim = new ClusterOptimizer(CpuFastQSizeRatio.of(5), filename -> 0d);
        victim.optimize(SAMPLE_WITH_TWO_LANES);
    }

    @Test
    public void providesEnoughCpusForRatioAndSplitsBetweenPrimaryAndPreemptibleWorkers() {
        PerformanceProfile profile = victim.optimize(SAMPLE_WITH_TWO_LANES);
        assertThat(profile.numPrimaryWorkers()).isEqualTo(3);
        assertThat(profile.numPreemtibleWorkers()).isEqualTo(3);
    }

    @Test
    public void usesDefaultWorkerAndMasterTypes() {
        PerformanceProfile profile = victim.optimize(SAMPLE_WITH_TWO_LANES);
        assertThat(profile.primaryWorkers()).isEqualTo(MachineType.defaultWorker());
        assertThat(profile.master()).isEqualTo(MachineType.defaultMaster());
    }

    @Test
    public void cpusFlooredForVerySmallFiles() {
        victim = new ClusterOptimizer(CpuFastQSizeRatio.of(5), filename -> 0.001d);
        PerformanceProfile profile = victim.optimize(SAMPLE_WITH_TWO_LANES);
        assertThat(profile.numPrimaryWorkers()).isEqualTo(2);
        assertThat(profile.numPreemtibleWorkers()).isEqualTo(0);
    }
}