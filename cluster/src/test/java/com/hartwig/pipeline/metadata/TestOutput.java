package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface TestOutput extends StageOutput {

    @Value.Default
    default String name(){
        return "test";
    }

    static ImmutableTestOutput.Builder builder() {
        return ImmutableTestOutput.builder();
    }
}
