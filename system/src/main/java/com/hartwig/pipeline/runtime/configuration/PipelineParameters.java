package com.hartwig.pipeline.runtime.configuration;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutablePipelineParameters.class)
@Value.Immutable
public interface PipelineParameters {

    Configuration.Flavour flavour();

    @Value.Default
    default boolean callGermline() {
        return false;
    }

    @Value.Default
    default BwaParameters bwa() {
        return ImmutableBwaParameters.builder().threads(12).build();
    }
}
