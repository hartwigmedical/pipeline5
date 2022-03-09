package com.hartwig.pipeline.calling.structural.gridss;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface GridssOutput extends StageOutput {

    @Override
    default String name() {
        return Gridss.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeUnfilteredVcf();

    Optional<GoogleStorageLocation> maybeUnfilteredVcfIndex();

    default GoogleStorageLocation unfilteredVariants() {
        return maybeUnfilteredVcf().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableGridssOutput.Builder builder() {
        return ImmutableGridssOutput.builder();
    }
}
