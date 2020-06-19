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
    default List<Subject> subjects() {
        List<Subject> subjects = PipelineEvent.super.subjects();
        subjects.add(Subject.of(status(), "status"));
        return subjects;
    }

    static ImmutablePipelineCompleted.Builder builder() {
        return ImmutablePipelineCompleted.builder();
    }
}
