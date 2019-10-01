package com.hartwig.pipeline.tertiary.bachelor;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface BachelorOutput extends StageOutput{

    @Override
    default String name() {
        return Bachelor.NAMESPACE;
    }

    static ImmutableBachelorOutput.Builder builder() {
        return ImmutableBachelorOutput.builder();
    }
}
