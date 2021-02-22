package com.hartwig.pipeline.tertiary.bachelor;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface BachelorOutput extends StageOutput {

    @Override
    default String name() {
        return Bachelor.NAMESPACE;
    }

    static ImmutableBachelorOutput.Builder builder() {
        return ImmutableBachelorOutput.builder();
    }

    Optional<GoogleStorageLocation> maybeReportableVariants();

    default GoogleStorageLocation reportableVariants() {
        return maybeReportableVariants().orElseThrow();
    }
}
