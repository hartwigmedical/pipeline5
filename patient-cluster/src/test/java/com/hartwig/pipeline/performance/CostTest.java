package com.hartwig.pipeline.performance;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.data.Offset;
import org.junit.Test;

public class CostTest {

    @Test
    public void costIsMachineCostPlusDataprocOverheadForEstimateOfFourHours() {
        Cost victim = new Cost();
        assertThat(victim.calculate(PerformanceProfile.builder().numPrimaryWorkers(1).numPreemtibleWorkers(1).build())).isEqualTo(14.26,
                Offset.offset(0.01));
    }
}