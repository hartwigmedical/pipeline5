package com.hartwig.pipeline.tertiary.amber;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface AmberOutput extends StageOutput {

    @Override
    default String name() {
        return Amber.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableAmberOutput.Builder builder() {
        return ImmutableAmberOutput.builder();
    }
}
