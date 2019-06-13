package com.hartwig.pipeline.tertiary.amber;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface AmberOutput extends StageOutput {

    @Override
    default String name() {
        return Amber.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElseThrow(() -> new IllegalStateException("No output directory available"));
    }

    static ImmutableAmberOutput.Builder builder() {
        return ImmutableAmberOutput.builder();
    }
}
