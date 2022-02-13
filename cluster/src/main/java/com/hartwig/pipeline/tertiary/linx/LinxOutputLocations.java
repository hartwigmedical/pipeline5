package com.hartwig.pipeline.tertiary.linx;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxOutputLocations {

    GoogleStorageLocation fusions();

    GoogleStorageLocation breakends();

    GoogleStorageLocation svAnnotations();

    GoogleStorageLocation clusters();

    GoogleStorageLocation driverCatalog();

    GoogleStorageLocation drivers();

    GoogleStorageLocation outputDirectory();

    static ImmutableLinxOutputLocations.Builder builder() {
        return ImmutableLinxOutputLocations.builder();
    }
}
