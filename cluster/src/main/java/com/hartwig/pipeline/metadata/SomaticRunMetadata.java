package com.hartwig.pipeline.metadata;

import org.immutables.value.Value;

@Value.Immutable
public interface SomaticRunMetadata {

    String runName();

    SingleSampleRunMetadata reference();

    SingleSampleRunMetadata tumor();

    static ImmutableSomaticRunMetadata.Builder builder() {
        return ImmutableSomaticRunMetadata.builder();
    }
}
