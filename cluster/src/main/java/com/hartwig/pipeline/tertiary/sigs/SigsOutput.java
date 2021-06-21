package com.hartwig.pipeline.tertiary.sigs;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.tertiary.sigs.ImmutableSigsOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface SigsOutput extends StageOutput {
    static ImmutableSigsOutput.Builder builder() {
        return ImmutableSigsOutput.builder();
    }

    @Override
    default String name() {
        return Sigs.NAMESPACE;
    }
}
