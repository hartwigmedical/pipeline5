package com.hartwig.pipeline.tertiary.lilac;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface LilacOutput extends StageOutput {
    @Override
    default String name() {
        return Lilac.NAMESPACE;
    }

    static ImmutableLilacOutput.Builder builder() {
        return ImmutableLilacOutput.builder();
    }
}
