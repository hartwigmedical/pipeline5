package com.hartwig.pipeline.calling.structural.gripss;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface GripssOutput extends StageOutput {

    Optional<GoogleStorageLocation> maybeFilteredVariants();

    Optional<GoogleStorageLocation> maybeUnfilteredVariants();

    default GoogleStorageLocation filteredVariants() {
        return maybeFilteredVariants().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation unfilteredVariants() {
        return maybeUnfilteredVariants().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableGripssOutput.Builder builder() {
        return ImmutableGripssOutput.builder();
    }
}
