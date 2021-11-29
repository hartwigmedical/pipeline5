package com.hartwig.pipeline.tertiary.pave;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PaveOutputLocations
{
    GoogleStorageLocation outputDirectory();

    GoogleStorageLocation somaticVcf();

    GoogleStorageLocation germlineVcf();

    static ImmutablePaveOutputLocations.Builder builder() {
        return ImmutablePaveOutputLocations.builder();
    }
}
