package com.hartwig.pipeline.flagstat;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface FlagstatOutput extends StageOutput {

    @Override
    default String name() {
        return Flagstat.NAMESPACE;
    }

    static String outputFile(Sample sample) {
        return String.format("%s.flagstat", sample.name());
    }

    static ImmutableFlagstatOutput.Builder builder() {
        return ImmutableFlagstatOutput.builder();
    }
}
