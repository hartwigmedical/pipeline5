package com.hartwig.bcl2fastq.stats;

public class TestStats {

    public static Stats empty() {
        return ImmutableStats.builder().flowcell("test").build();
    }

    public static ImmutableStats stats(final LaneStats conversionresults) {
        return ImmutableStats.builder().flowcell("test").addConversionResults(conversionresults).build();
    }

    public static ImmutableLaneStats laneStats(final int laneNumber, final int undeterminedYield, final SampleStats... demuxStats) {
        return ImmutableLaneStats.builder()
                .undetermined(ImmutableUndeterminedStats.builder().yield(undeterminedYield).build())
                .laneNumber(laneNumber)
                .addDemuxResults(demuxStats)
                .build();
    }
}
