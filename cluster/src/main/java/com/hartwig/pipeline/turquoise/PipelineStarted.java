package com.hartwig.pipeline.turquoise;

import org.immutables.value.Value;

@Value.Immutable
public interface PipelineStarted extends PipelineEvent {

    @Override
    default String eventType() {
        return "pipeline.started";
    }

    static ImmutablePipelineStarted.Builder builder() {
        return ImmutablePipelineStarted.builder();
    }
}
