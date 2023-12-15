package com.hartwig.pipeline.tertiary.purple;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutputLocations {

    GoogleStorageLocation purity();

    GoogleStorageLocation qcFile();

    GoogleStorageLocation outputDirectory();

    // somatic
    Optional<GoogleStorageLocation> somaticVariants();

    Optional<GoogleStorageLocation> structuralVariants();

    Optional<GoogleStorageLocation> geneCopyNumber();

    Optional<GoogleStorageLocation> somaticDriverCatalog();

    Optional<GoogleStorageLocation> somaticCopyNumber();

    Optional<GoogleStorageLocation> circosPlot();

    // germline
    Optional<GoogleStorageLocation> germlineVariants();

    Optional<GoogleStorageLocation> germlineStructuralVariants();

    Optional<GoogleStorageLocation> germlineDriverCatalog();

    Optional<GoogleStorageLocation> germlineDeletions();

    static ImmutablePurpleOutputLocations.Builder builder() {
        return ImmutablePurpleOutputLocations.builder();
    }
}
