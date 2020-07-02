package com.hartwig.pipeline.turquoise;

import java.util.List;

import org.immutables.value.Value;

@Value.Immutable
public interface PipelineCompleted extends PipelineEvent {

    String status();

    @Override
    default String eventType() {
        return "pipeline.completed";
    }

    @Override
    default List<Label> labels() {
        List<Label> labels = PipelineEvent.super.labels();
        labels.add(Label.of("status", status()));
        return labels;
    }

    static ImmutablePipelineCompleted.Builder builder() {
        return ImmutablePipelineCompleted.builder();
    }
}
