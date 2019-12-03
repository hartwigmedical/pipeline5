package com.hartwig.bcl2fastq;

import org.immutables.value.Value;

@Value.Immutable
public interface FastqId {

    @Value.Parameter
    int lane();

    @Value.Parameter
    String sample();

    static FastqId of(int lane, String sample) {
        return ImmutableFastqId.of(lane, sample);
    }
}
