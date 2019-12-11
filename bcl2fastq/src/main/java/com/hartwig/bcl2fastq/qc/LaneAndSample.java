package com.hartwig.bcl2fastq.qc;

import com.hartwig.bcl2fastq.FastqId;
import com.hartwig.bcl2fastq.stats.SampleStats;

import org.immutables.value.Value;

@Value.Immutable
interface LaneAndSample {

    @Value.Parameter
    int lane();

    @Value.Parameter
    SampleStats sample();

    default FastqId toFastqId() {
        return FastqId.of(lane(), sample().sampleId().orElseThrow());
    }

    static LaneAndSample of(int lane, SampleStats sample) {
        return ImmutableLaneAndSample.of(lane, sample);
    }
}
