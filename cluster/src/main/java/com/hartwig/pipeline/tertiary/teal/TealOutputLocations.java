package com.hartwig.pipeline.tertiary.teal;

import java.util.Optional;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface TealOutputLocations {
    Optional<GoogleStorageLocation> germlineTellength();

    Optional<GoogleStorageLocation> germlineTelbam();

    Optional<GoogleStorageLocation> somaticTellength();

    Optional<GoogleStorageLocation> somaticTelbam();

    Optional<GoogleStorageLocation> somaticBreakend();

    static ImmutableTealOutputLocations.Builder builder() {
        return ImmutableTealOutputLocations.builder();
    }
}
