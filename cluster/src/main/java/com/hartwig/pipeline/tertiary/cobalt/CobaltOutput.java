package com.hartwig.pipeline.tertiary.cobalt;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CobaltOutput extends StageOutput {

    @Override
    default String name() {
        return Cobalt.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElseThrow(() -> new IllegalStateException("No output directory available"));
    }

    static ImmutableCobaltOutput.Builder builder() {
        return ImmutableCobaltOutput.builder();
    }
}
