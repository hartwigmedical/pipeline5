package com.hartwig.pipeline.tertiary.virus;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusInterpreterOutput extends StageOutput {

    @Override
    default String name() {
        return VirusInterpreter.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeVirusAnnotations();

    default GoogleStorageLocation virusAnnotations() {
        return maybeVirusAnnotations().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableVirusInterpreterOutput.Builder builder() {
        return ImmutableVirusInterpreterOutput.builder();
    }
}