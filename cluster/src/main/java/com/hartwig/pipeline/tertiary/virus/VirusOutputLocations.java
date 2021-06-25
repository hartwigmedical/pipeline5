package com.hartwig.pipeline.tertiary.virus;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface VirusOutputLocations {

    GoogleStorageLocation summaryFile();

    GoogleStorageLocation annotatedVirusFile();

    static ImmutableVirusOutputLocations.Builder builder() {
        return ImmutableVirusOutputLocations.builder();
    }
}
