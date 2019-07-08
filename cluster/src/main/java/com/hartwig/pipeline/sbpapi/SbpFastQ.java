package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

@JsonDeserialize(as = ImmutableSbpFastQ.class)
@Value.Immutable
public interface SbpFastQ {

    String name_r1();

    String name_r2();

    @Nullable
    String bucket();

    boolean qc_pass();
}
