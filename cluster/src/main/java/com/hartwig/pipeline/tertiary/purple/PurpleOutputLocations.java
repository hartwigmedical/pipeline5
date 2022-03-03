package com.hartwig.pipeline.tertiary.purple;

import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutputLocations {
    GoogleStorageLocation circosPlot();

    GoogleStorageLocation outputDirectory();

    GoogleStorageLocation somaticVariants();

    GoogleStorageLocation germlineVariants();

    GoogleStorageLocation structuralVariants();

    GoogleStorageLocation purity();

    GoogleStorageLocation qcFile();

    GoogleStorageLocation geneCopyNumber();

    GoogleStorageLocation somaticDriverCatalog();

    GoogleStorageLocation somaticCopyNumber();

    GoogleStorageLocation germlineDriverCatalog();

    static ImmutablePurpleOutputLocations.Builder builder() {
        return ImmutablePurpleOutputLocations.builder();
    }
}
