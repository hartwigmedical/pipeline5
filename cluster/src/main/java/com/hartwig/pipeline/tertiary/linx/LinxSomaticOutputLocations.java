package com.hartwig.pipeline.tertiary.linx;

import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxSomaticOutputLocations {

    GoogleStorageLocation fusions();

    GoogleStorageLocation breakends();

    GoogleStorageLocation svAnnotations();

    GoogleStorageLocation clusters();

    GoogleStorageLocation driverCatalog();

    GoogleStorageLocation drivers();

    GoogleStorageLocation outputDirectory();

    static ImmutableLinxSomaticOutputLocations.Builder builder() {
        return ImmutableLinxSomaticOutputLocations.builder();
    }
}
