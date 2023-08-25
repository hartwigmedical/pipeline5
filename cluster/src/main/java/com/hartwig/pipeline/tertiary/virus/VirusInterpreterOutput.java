package com.hartwig.pipeline.tertiary.virus;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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