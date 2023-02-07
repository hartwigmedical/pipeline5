package com.hartwig.pipeline.tertiary.linx;

import static com.hartwig.pipeline.storage.GoogleStorageLocation.empty;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface LinxGermlineOutput extends StageOutput {

    @Override
    default String name() {
        return LinxGermline.NAMESPACE;
    }

    static ImmutableLinxGermlineOutput.Builder builder() {
        return ImmutableLinxGermlineOutput.builder();
    }

    Optional<LinxGermlineOutputLocations> maybeLinxGermlineOutputLocations();

    default LinxGermlineOutputLocations linxOutputLocations() {
        return maybeLinxGermlineOutputLocations().orElse(LinxGermlineOutputLocations.builder()
                .disruptions(empty())
                .breakends(empty())
                .driverCatalog(empty())
                .outputDirectory(empty())
                .build());
    }
}
