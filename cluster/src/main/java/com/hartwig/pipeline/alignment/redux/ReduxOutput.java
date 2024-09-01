package com.hartwig.pipeline.alignment.redux;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.tertiary.cobalt.Cobalt;
import com.hartwig.pipeline.tertiary.cobalt.ImmutableCobaltOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface ReduxOutput extends StageOutput {

    @Override
    default String name() {
        return Cobalt.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableReduxOutput.Builder builder() {
        return ImmutableReduxOutput.builder();
    }
}
