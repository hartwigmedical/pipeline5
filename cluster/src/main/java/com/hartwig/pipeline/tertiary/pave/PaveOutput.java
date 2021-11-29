package com.hartwig.pipeline.tertiary.pave;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface PaveOutput extends StageOutput {

    @Override
    default String name() {
        return Pave.NAMESPACE;
    }

    Optional<PaveOutputLocations> maybeOutputLocations();

    default PaveOutputLocations outputLocations() {
        return maybeOutputLocations().orElseThrow();
    }

    static ImmutablePaveOutput.Builder builder() {
        return ImmutablePaveOutput.builder();
    }
}
