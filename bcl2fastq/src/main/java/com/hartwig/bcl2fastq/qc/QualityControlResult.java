package com.hartwig.bcl2fastq.qc;

import org.immutables.value.Value;

@Value.Immutable
public interface QualityControlResult {

    String name();

    boolean pass();

    default boolean fail() {
        return !pass();
    }

    static QualityControlResult of(String name, boolean pass) {
        return ImmutableQualityControlResult.builder().pass(pass).name(name).build();
    }
}
