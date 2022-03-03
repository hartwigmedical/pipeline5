package com.hartwig.pipeline.calling.structural;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface StructuralCallerOutput extends StageOutput {

    @Override
    default String name() {
        return StructuralCaller.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeUnfilteredVcf();

    Optional<GoogleStorageLocation> maybeUnfilteredVcfIndex();

    default GoogleStorageLocation unfilteredVariants() {
        return maybeUnfilteredVcf().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableStructuralCallerOutput.Builder builder() {
        return ImmutableStructuralCallerOutput.builder();
    }
}
