package com.hartwig.pipeline.metadata;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSomaticRunMetadata.class)
@Value.Immutable
public interface SomaticRunMetadata {

    String runName();

    SingleSampleRunMetadata reference();

    SingleSampleRunMetadata tumor();

    static ImmutableSomaticRunMetadata.Builder builder() {
        return ImmutableSomaticRunMetadata.builder();
    }
}
