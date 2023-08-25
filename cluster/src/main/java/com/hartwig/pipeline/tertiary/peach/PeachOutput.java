package com.hartwig.pipeline.tertiary.peach;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface PeachOutput extends StageOutput {
    static ImmutablePeachOutput.Builder builder() {
        return ImmutablePeachOutput.builder();
    }

    @Override
    default String name() {
        return Peach.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeGenotypes();

    default GoogleStorageLocation genotypes() {
        return maybeGenotypes().orElse(GoogleStorageLocation.empty());
    }
}