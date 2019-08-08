package com.hartwig.pipeline.metadata;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSomaticRunMetadata.class)
@Value.Immutable
public interface SomaticRunMetadata {

    String runName();

    SingleSampleRunMetadata reference();

    Optional<SingleSampleRunMetadata> maybeTumor();

    default SingleSampleRunMetadata tumor() {
        return maybeTumor().orElseThrow(() -> new IllegalStateException(
                "No tumor is present in this run/set. Somatic algorithms should not be called."));
    }

    static ImmutableSomaticRunMetadata.Builder builder() {
        return ImmutableSomaticRunMetadata.builder();
    }
}