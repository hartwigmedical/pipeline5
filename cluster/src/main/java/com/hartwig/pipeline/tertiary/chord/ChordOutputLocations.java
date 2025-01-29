package com.hartwig.pipeline.tertiary.chord;

import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface ChordOutputLocations
{
    static ImmutableChordOutputLocations.Builder builder() {
        return ImmutableChordOutputLocations.builder();
    }

    GoogleStorageLocation predictions();

    GoogleStorageLocation signatures();
}