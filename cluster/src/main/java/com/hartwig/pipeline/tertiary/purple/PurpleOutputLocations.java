package com.hartwig.pipeline.tertiary.purple;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutputLocations {

    GoogleStorageLocation outputDirectory();

    GoogleStorageLocation somaticVcf();

    GoogleStorageLocation germlineVcf();

    GoogleStorageLocation structuralVcf();

    GoogleStorageLocation purityTsv();

    GoogleStorageLocation qcFile();

    GoogleStorageLocation somaticDriverCatalog();

    GoogleStorageLocation germlineDriverCatalog();

    static ImmutablePurpleOutputLocations.Builder builder() {
        return ImmutablePurpleOutputLocations.builder();
    }
}
