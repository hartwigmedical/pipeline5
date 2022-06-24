package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

@JsonDeserialize(as = ImmutableSbpFastQ.class)
@Value.Immutable
public interface SbpFastQ {

    int sample_id();

    String name_r1();

    String name_r2();

    @Nullable String bucket();

    boolean qc_pass();

    int lane_id();

    static ImmutableSbpFastQ.Builder builder() {
        return ImmutableSbpFastQ.builder();
    }
}
