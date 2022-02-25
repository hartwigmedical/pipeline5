package com.hartwig.pipeline.tertiary.linx;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxSomaticOutput extends StageOutput {

    @Override
    default String name() {
        return LinxSomatic.NAMESPACE;
    }

    static ImmutableLinxSomaticOutput.Builder builder() {
        return ImmutableLinxSomaticOutput.builder();
    }

    Optional<LinxSomaticOutputLocations> maybeLinxOutputLocations();

    default LinxSomaticOutputLocations linxOutputLocations() {
        return maybeLinxOutputLocations().orElse(LinxSomaticOutputLocations.builder()
                .drivers(GoogleStorageLocation.empty())
                .breakends(GoogleStorageLocation.empty())
                .outputDirectory(GoogleStorageLocation.empty())
                .clusters(GoogleStorageLocation.empty())
                .driverCatalog(GoogleStorageLocation.empty())
                .svAnnotations(GoogleStorageLocation.empty())
                .fusions(GoogleStorageLocation.empty())
                .build());
    }
}
