package com.hartwig.pipeline.tertiary.purple;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PurpleOutput extends StageOutput {

    @Override
    default String name() {
        return Purple.NAMESPACE;
    }

    Optional<PurpleOutputLocations> maybeOutputLocations();

    default PurpleOutputLocations outputLocations() {
        return maybeOutputLocations().orElse(PurpleOutputLocations.builder()
                .structuralVariants(GoogleStorageLocation.empty())
                .somaticVariants(GoogleStorageLocation.empty())
                .germlineVariants(GoogleStorageLocation.empty())
                .qcFile(GoogleStorageLocation.empty())
                .purity(GoogleStorageLocation.empty())
                .germlineDriverCatalog(GoogleStorageLocation.empty())
                .circosPlot(GoogleStorageLocation.empty())
                .somaticCopyNumber(GoogleStorageLocation.empty())
                .geneCopyNumber(GoogleStorageLocation.empty())
                .germlineDeletions(GoogleStorageLocation.empty())
                .outputDirectory(GoogleStorageLocation.empty())
                .somaticDriverCatalog(GoogleStorageLocation.empty())
                .build());
    }

    static ImmutablePurpleOutput.Builder builder() {
        return ImmutablePurpleOutput.builder();
    }
}
