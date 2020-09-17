package com.hartwig.pipeline.sbpapi;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSbpRun.class)
public interface SbpRun {

    String id();

    int ini_id();

    Optional<String> bucket();

    SbpSet set();

    String status();
}