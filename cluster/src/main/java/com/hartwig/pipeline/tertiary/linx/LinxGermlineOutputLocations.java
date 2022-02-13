package com.hartwig.pipeline.tertiary.linx;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxGermlineOutputLocations
{
    GoogleStorageLocation disruptions();

    GoogleStorageLocation driverCatalog();

    GoogleStorageLocation outputDirectory();

    static ImmutableLinxGermlineOutputLocations.Builder builder() {
        return ImmutableLinxGermlineOutputLocations.builder();
    }
}
