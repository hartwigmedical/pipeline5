package com.hartwig.bcl2fastq.qc;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.bcl2fastq.stats.ImmutableSampleStats;
import com.hartwig.bcl2fastq.stats.ImmutableStats;
import com.hartwig.bcl2fastq.stats.TestStats;

import org.junit.Before;
import org.junit.Test;

public class UndeterminedReadPercentageTest {

    private UndeterminedReadPercentage victim;

    @Before
    public void setUp() {
        victim = new UndeterminedReadPercentage(1);
    }

    @Test
    public void passesWhenLessThanThresholdUndetermined() {
        QualityControlResult result = victim.apply(stats(1000, 1), "");
        assertThat(result.pass()).isTrue();
    }

    @Test
    public void failsWhenMoreThanThresholdUndetermined() {
        QualityControlResult result = victim.apply(stats(50, 2), "");
        assertThat(result.pass()).isFalse();
    }

    private ImmutableStats stats(final int sampleYield, final int undeterminedYield) {
        return TestStats.stats(TestStats.laneStats(1, undeterminedYield, ImmutableSampleStats.builder().yield(sampleYield).build()));
    }
}