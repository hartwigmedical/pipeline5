package com.hartwig.bcl2fastq.qc;

import com.hartwig.bcl2fastq.stats.ImmutableLaneStats;
import com.hartwig.bcl2fastq.stats.ImmutableStats;
import com.hartwig.bcl2fastq.stats.ImmutableUndeterminedStats;
import com.hartwig.bcl2fastq.stats.LaneStats;
import com.hartwig.bcl2fastq.stats.SampleStats;
import com.hartwig.bcl2fastq.stats.Stats;

class TestStats {

    static Stats empty() {
        return ImmutableStats.builder().flowcell("test").build();
    }

    static ImmutableStats stats(final LaneStats conversionresults) {
        return ImmutableStats.builder().flowcell("test").addConversionResults(conversionresults).build();
    }

    static ImmutableLaneStats laneStats(final int laneNumber, final int undeterminedYield, final SampleStats... demuxStats) {
        return ImmutableLaneStats.builder()
                .undetermined(ImmutableUndeterminedStats.builder().yield(undeterminedYield).build())
                .laneNumber(laneNumber)
                .addDemuxResults(demuxStats)
                .build();
    }
}
