package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface GermlineCallerOutput extends StageOutput{

    @Override
    default String name() {
        return GermlineCaller.NAMESPACE;
    }

    static ImmutableGermlineCallerOutput.Builder builder() {
        return ImmutableGermlineCallerOutput.builder();
    }
}