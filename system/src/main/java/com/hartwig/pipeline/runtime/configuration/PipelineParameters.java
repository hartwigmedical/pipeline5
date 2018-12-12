package com.hartwig.pipeline.runtime.configuration;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutablePipelineParameters.class)
@Value.Immutable
@ParameterStyle
public interface PipelineParameters {

    @Value.Default
    default String resultsDirectory() {
        return "/results/";
    }

    @Value.Default
    default String hdfs() {
        return "file:///";
    }

    @Value.Default
    default boolean saveResultsAsSingleFile() {
        return false;
    }

    @Value.Default
    default BwaParameters bwa() {
        return ImmutableBwaParameters.builder().threads(12).build();
    }

    static ImmutablePipelineParameters.Builder builder() {
        return ImmutablePipelineParameters.builder();
    }
}
