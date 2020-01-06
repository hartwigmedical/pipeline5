package com.hartwig.bcl2fastq.conversion;

import org.immutables.value.Value;

@Value.Immutable
public interface FastqId {

    @Value.Parameter
    int lane();

    @Value.Parameter
    String barcode();

    static FastqId of(int lane, String barcode) {
        return ImmutableFastqId.of(lane, barcode);
    }
}
