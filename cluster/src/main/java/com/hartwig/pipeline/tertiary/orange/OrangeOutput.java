package com.hartwig.pipeline.tertiary.orange;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface OrangeOutput extends StageOutput {
    static ImmutableOrangeOutput.Builder builder() {
        return ImmutableOrangeOutput.builder();
    }

    @Override
    default String name() {
        return Orange.NAMESPACE;
    }

}
