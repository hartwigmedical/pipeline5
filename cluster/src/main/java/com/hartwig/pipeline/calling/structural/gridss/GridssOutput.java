package com.hartwig.pipeline.calling.structural.gridss;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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
