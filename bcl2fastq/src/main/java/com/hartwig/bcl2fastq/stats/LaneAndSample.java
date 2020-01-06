package com.hartwig.bcl2fastq.stats;

import com.hartwig.bcl2fastq.conversion.FastqId;

import org.immutables.value.Value;

@Value.Immutable
interface LaneAndSample {

    @Value.Parameter
    int lane();

    @Value.Parameter
    SampleStats sample();

    default FastqId toFastqId() {
        return FastqId.of(lane(), sample().barcode());
    }

    static LaneAndSample of(int lane, SampleStats sample) {
        return ImmutableLaneAndSample.of(lane, sample);
    }
}
