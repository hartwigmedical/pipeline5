package com.hartwig.pipeline.tertiary.cider;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface CiderOutputLocations {
    Optional<GoogleStorageLocation> vdj();

    Optional<GoogleStorageLocation> locusStats();

    Optional<GoogleStorageLocation> layouts();

    Optional<GoogleStorageLocation> ciderBam();

    static ImmutableCiderOutputLocations.Builder builder() {
        return ImmutableCiderOutputLocations.builder();
    }
}
