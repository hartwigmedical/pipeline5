package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSbpLane.class)
@JsonSerialize(as = ImmutableSbpLane.class)
@Value.Immutable
@Value.Style(jdkOnly = true)
public interface SbpLane {

    int id();

    String name();
}
