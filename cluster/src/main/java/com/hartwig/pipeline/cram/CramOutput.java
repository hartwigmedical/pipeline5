package com.hartwig.pipeline.cram;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface CramOutput extends StageOutput {
    static String craiFile(String cram) {
        return cram + ".crai";
    }

    static ImmutableCramOutput.Builder builder() {
        return ImmutableCramOutput.builder();
    }

    @Override
    default String name() {
        return CramConversion.NAMESPACE;
    }
}
