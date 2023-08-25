package com.hartwig.pipeline.tertiary.linx;

import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

import static com.hartwig.computeengine.storage.GoogleStorageLocation.empty;

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
