package com.hartwig.pipeline.metadata;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSbpRun.class)
public interface SbpRun {

    String id();

    SbpSet set();
}
