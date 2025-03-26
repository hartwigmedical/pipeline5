package com.hartwig.pipeline.tertiary.vchord;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface VChordOutputLocations {
    Optional<GoogleStorageLocation> vChordPrediction();

    static ImmutableVChordOutputLocations.Builder builder() {
        return ImmutableVChordOutputLocations.builder();
    }
}
