package com.hartwig.pipeline.tertiary.pave;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface PaveOutput extends StageOutput {

    Optional<GoogleStorageLocation> maybeAnnotatedVariants();

    default GoogleStorageLocation annotatedVariants() {
        return maybeAnnotatedVariants().orElse(GoogleStorageLocation.empty());
    }

    static ImmutablePaveOutput.Builder builder(final String namespace) {
        return ImmutablePaveOutput.builder().name(namespace);
    }
}
