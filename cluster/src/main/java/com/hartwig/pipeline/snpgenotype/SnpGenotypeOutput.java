package com.hartwig.pipeline.snpgenotype;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface SnpGenotypeOutput extends StageOutput {

    @Override
    default String name() {
        return SnpGenotype.NAMESPACE;
    }

    static ImmutableSnpGenotypeOutput.Builder builder() {
        return ImmutableSnpGenotypeOutput.builder();
    }
}