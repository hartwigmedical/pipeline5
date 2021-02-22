package com.hartwig.pipeline.tertiary.purple;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutputLocations {

    GoogleStorageLocation outputDirectory();

    GoogleStorageLocation somaticVcf();

    GoogleStorageLocation structuralVcf();

    GoogleStorageLocation purityTsv();

    GoogleStorageLocation qcFile();

    GoogleStorageLocation driverCatalog();

    static ImmutablePurpleOutputLocations.Builder builder() {
        return ImmutablePurpleOutputLocations.builder();
    }
}
