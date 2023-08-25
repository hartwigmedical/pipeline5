package com.hartwig.pipeline.calling.structural.gripss;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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

    static ImmutableGripssOutput.Builder builder(final String namespace) {
        return ImmutableGripssOutput.builder().name(namespace);
    }
}
