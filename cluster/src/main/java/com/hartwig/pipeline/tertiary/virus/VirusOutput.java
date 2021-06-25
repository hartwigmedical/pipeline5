package com.hartwig.pipeline.tertiary.virus;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusOutput extends StageOutput {

    @Override
    default String name() {
        return VirusAnalysis.NAMESPACE;
    }

    Optional<VirusOutputLocations> maybeOutputLocations();

    default VirusOutputLocations outputLocations() {
        return maybeOutputLocations().orElseThrow();
    }

    static ImmutableVirusOutput.Builder builder() {
        return ImmutableVirusOutput.builder();
    }
}
