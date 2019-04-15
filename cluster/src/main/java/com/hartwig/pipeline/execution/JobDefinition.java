package com.hartwig.pipeline.execution;

public interface JobDefinition<T extends PerformanceProfile> {

    String name();

    T performanceProfile();
}
