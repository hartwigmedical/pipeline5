package com.hartwig.pipeline.tertiary.purple;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutputLocations {
    GoogleStorageLocation circosPlot();

    GoogleStorageLocation outputDirectory();

    GoogleStorageLocation somaticVcf();

    GoogleStorageLocation germlineVcf();

    GoogleStorageLocation structuralVcf();

    GoogleStorageLocation purityTsv();

    GoogleStorageLocation qcFile();

    GoogleStorageLocation geneCopyNumberTsv();

    GoogleStorageLocation somaticDriverCatalog();

    GoogleStorageLocation somaticCopyNumberTsv();

    GoogleStorageLocation germlineDriverCatalog();

    static ImmutablePurpleOutputLocations.Builder builder() {
        return ImmutablePurpleOutputLocations.builder();
    }
}
