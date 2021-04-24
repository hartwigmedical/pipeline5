package com.hartwig.pipeline.tertiary.peach;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface PeachOutput extends StageOutput {
    static ImmutablePeachOutput.Builder builder() {
        return ImmutablePeachOutput.builder();
    }

    @Override
    default String name() {
        return Peach.NAMESPACE;
    }
}
