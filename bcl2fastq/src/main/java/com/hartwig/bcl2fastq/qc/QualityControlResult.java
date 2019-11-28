package com.hartwig.bcl2fastq.qc;

import org.immutables.value.Value;

@Value.Immutable
public interface QualityControlResult {

    boolean pass();

    static QualityControlResult of(boolean pass) {
        return ImmutableQualityControlResult.builder().pass(pass).build();
    }
}
