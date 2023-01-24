package com.hartwig.pipeline.input;

import org.immutables.value.Value;

@Value.Immutable
public interface ExternalIds {

    Long runId();

    Long setId();

    static ImmutableExternalIds.Builder builder() {
        return ImmutableExternalIds.builder();
    }
}
