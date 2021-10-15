package com.hartwig.pipeline.tertiary.virus;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusOutput extends StageOutput {

    @Override
    default String name() {
        return VirusAnalysis.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeAnnotatedVirusFile();

    default GoogleStorageLocation annotatedVirusFile() {
        return maybeAnnotatedVirusFile().orElseThrow();
    }

    static ImmutableVirusOutput.Builder builder() {
        return ImmutableVirusOutput.builder();
    }
}