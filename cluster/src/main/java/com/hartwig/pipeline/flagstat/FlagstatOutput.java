package com.hartwig.pipeline.flagstat;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface FlagstatOutput extends StageOutput {

    @Override
    default String name() {
        return Flagstat.NAMESPACE;
    }

    static String outputFile(String sample) {
        return String.format("%s.flagstat", sample);
    }

    static ImmutableFlagstatOutput.Builder builder() {
        return ImmutableFlagstatOutput.builder();
    }
}
