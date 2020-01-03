package com.hartwig.bcl2fastq.stats;

public class TestStats {

    public static Stats empty() {
        return ImmutableStats.builder().flowcell("test").build();
    }

    public static ImmutableStats stats(final String flowcell, final LaneStats ... conversionresults) {
        return ImmutableStats.builder().flowcell(flowcell).addConversionResults(conversionresults).build();
    }

    public static ImmutableLaneStats laneStats(final int laneNumber, final int undeterminedYield, final SampleStats... demuxStats) {
        return ImmutableLaneStats.builder()
                .undetermined(ImmutableUndeterminedStats.builder().yield(undeterminedYield).build())
                .laneNumber(laneNumber)
                .addDemuxResults(demuxStats)
                .build();
    }

    public static ImmutableSampleStats sampleStats(final String barcode, final long yield, final long... q30s) {
        ImmutableSampleStats.Builder builder = ImmutableSampleStats.builder().barcode(barcode).yield(yield);
        int readNumber = 1;
        for (long q30 : q30s) {
            builder.addReadMetrics(ImmutableReadMetrics.builder().yield(q30).yieldQ30(q30).readNumber(readNumber).build());
            readNumber++;
        }
        return builder.build();
    }
}
