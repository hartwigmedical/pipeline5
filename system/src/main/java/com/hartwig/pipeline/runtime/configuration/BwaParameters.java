package com.hartwig.pipeline.runtime.configuration;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableBwaParameters.class)
@Value.Immutable
public interface BwaParameters {

    int threads();

    static ImmutableBwaParameters.Builder builder() {
        return ImmutableBwaParameters.builder();
    }
}
