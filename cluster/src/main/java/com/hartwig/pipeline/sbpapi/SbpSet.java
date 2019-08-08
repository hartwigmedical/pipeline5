package com.hartwig.pipeline.sbpapi;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSbpSet.class)
@Value.Immutable
public interface SbpSet {

    Optional<String> tumor_sample();

    String ref_sample();

    String name();

    String id();
}
