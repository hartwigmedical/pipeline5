package com.hartwig.pipeline.tertiary.peach;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

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