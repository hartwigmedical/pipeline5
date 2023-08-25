package com.hartwig.pipeline.tertiary.cobalt;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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
