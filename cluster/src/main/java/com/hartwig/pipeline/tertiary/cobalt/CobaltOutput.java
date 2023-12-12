package com.hartwig.pipeline.tertiary.cobalt;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CobaltOutput extends StageOutput {

    @Override
    default String name() {
        return Cobalt.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableCobaltOutput.Builder builder() {
        return ImmutableCobaltOutput.builder();
    }
}
