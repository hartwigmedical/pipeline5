package com.hartwig.bcl2fastq.stats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class AggregationsTest {

    @Test
    public void sumsYieldForSample() {
        Stats stats = TestStats.stats("test",
                TestStats.laneStats(1, 1, statsWithYield("barcode1", 1), statsWithYield("barcode2", 2), statsWithYield("barcode1", 3)));
        assertThat(Aggregations.yield("barcode1", stats)).isEqualTo(4);
    }

    private ImmutableSampleStats statsWithYield(final String barcode, final int yield) {
        return ImmutableSampleStats.builder().yield(yield).barcode(barcode).build();
    }
}