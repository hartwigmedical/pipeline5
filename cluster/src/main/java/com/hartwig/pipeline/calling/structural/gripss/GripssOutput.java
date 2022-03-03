package com.hartwig.pipeline.calling.structural.gripss;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public interface GripssOutput extends StageOutput {

    Optional<GoogleStorageLocation> maybeFilteredVariants();

    Optional<GoogleStorageLocation> maybeUnfilteredVariants();

    default GoogleStorageLocation filteredVariants() {
        return maybeFilteredVariants().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation unfilteredVariants() {
        return maybeUnfilteredVariants().orElse(GoogleStorageLocation.empty());
    }
}
