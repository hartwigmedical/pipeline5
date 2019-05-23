package com.hartwig.pipeline.tertiary.healthcheck;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutablePv4Metadata.class)
@Value.Immutable
public interface Pv4Metadata {

    String ref_sample();

    String tumor_sample();

    String set_name();

    static ImmutablePv4Metadata.Builder builder() {
        return ImmutablePv4Metadata.builder();
    }
}
