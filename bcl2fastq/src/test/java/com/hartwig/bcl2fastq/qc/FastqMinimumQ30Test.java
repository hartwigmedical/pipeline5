package com.hartwig.bcl2fastq.qc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import com.hartwig.bcl2fastq.FastqId;
import com.hartwig.bcl2fastq.stats.ImmutableReadMetrics;
import com.hartwig.bcl2fastq.stats.ImmutableSampleStats;
import com.hartwig.bcl2fastq.stats.ImmutableStats;
import com.hartwig.bcl2fastq.stats.SampleStats;
import com.hartwig.bcl2fastq.stats.TestStats;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class FastqMinimumQ30Test {

    private static final String SAMPLE_1 = "sample1";
    private static final String SAMPLE_2 = "sample2";
    private FastqMinimumQ30 victim;

    @Test
    public void emptyStatsReturnEmptyQualityControlMap() {
        FastqMinimumQ30 victim = new FastqMinimumQ30(10);
        assertThat(victim.apply(TestStats.empty())).isEmpty();
    }

    @Before
    public void setUp() {
        victim = new FastqMinimumQ30(1);
    }

    @Test
    public void q30ForFastqBelowMinimum() {
        Map<FastqId, QualityControlResult> result = victim.apply(stats(sample(1, 0, SAMPLE_1)));
        assertThat(result).hasSize(1);
        assertThat(result.keySet()).containsExactly(FastqId.of(1, SAMPLE_1));
        assertThat(result.values()).containsExactly(QualityControlResult.of(FastqMinimumQ30.QC_NAME, false));
    }

    @Test
    public void q30ForFastqAboveMinimum() {
        Map<FastqId, QualityControlResult> result = victim.apply(stats(sample(2, 0, SAMPLE_1)));
        assertThat(result).hasSize(1);
        assertThat(result.values()).containsExactly(QualityControlResult.of(FastqMinimumQ30.QC_NAME, true));
    }

    @Test
    public void sumsQ30AcrossReadMetrics() {
        Map<FastqId, QualityControlResult> result = victim.apply(stats(sample(1, 1, SAMPLE_1)));
        assertThat(result).hasSize(1);
        assertThat(result.values()).containsExactly(QualityControlResult.of(FastqMinimumQ30.QC_NAME, true));
    }

    @Test
    public void groupsResultsIntoFastqs() {
        Map<FastqId, QualityControlResult> result = victim.apply(stats(sample(1, 1, SAMPLE_1), sample(1, 1, SAMPLE_2)));
        assertThat(result).hasSize(2);
        assertThat(result.keySet()).containsExactlyInAnyOrder(FastqId.of(1, SAMPLE_1), FastqId.of(1, SAMPLE_2));
        assertThat(result.values()).allMatch(r -> r.equals(QualityControlResult.of(FastqMinimumQ30.QC_NAME, true)));
    }

    private ImmutableStats stats(final @NotNull SampleStats... sample) {
        return TestStats.stats(TestStats.laneStats(1, 0, sample));
    }

    @NotNull
    private ImmutableSampleStats sample(final int yieldQ30R1, final int yieldQ30R2, final String sample) {
        return ImmutableSampleStats.builder()
                .yield(1)
                .sampleId(sample)
                .addReadMetrics(readMetrics(yieldQ30R1), readMetrics(yieldQ30R2))
                .build();
    }

    @NotNull
    private ImmutableReadMetrics readMetrics(final int yieldQ30) {
        return ImmutableReadMetrics.builder().readNumber(1).yield(1).yieldQ30(yieldQ30).build();
    }
}