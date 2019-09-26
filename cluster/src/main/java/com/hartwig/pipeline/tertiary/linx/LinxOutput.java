package com.hartwig.pipeline.tertiary.linx;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxOutput extends StageOutput{

    @Override
    default String name() {
        return Linx.NAMESPACE;
    }

    static ImmutableLinxOutput.Builder builder() {
        return ImmutableLinxOutput.builder();
    }
}
