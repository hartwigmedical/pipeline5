package com.hartwig.pipeline.sbpapi;

import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSbpRun.class)
public interface SbpRun {

    String id();

    @Nullable
    String bucket();

    SbpSet set();
}