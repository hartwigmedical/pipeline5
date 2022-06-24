package com.hartwig.pipeline.tertiary.rose;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface RoseOutput extends StageOutput {
    @Override
    default String name() {
        return Rose.NAMESPACE;
    }

    static ImmutableRoseOutput.Builder builder() {
        return ImmutableRoseOutput.builder();
    }
}
