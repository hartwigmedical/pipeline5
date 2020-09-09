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

    default GoogleStorageLocation unfilteredVcf() {
        return maybeUnfilteredVcf().orElseThrow(() -> new IllegalStateException("No unfiltered VCF available"));
    }

    default GoogleStorageLocation unfilteredVcfIndex() {
        return maybeUnfilteredVcfIndex().orElseThrow(() -> new IllegalStateException("No unfiltered VCF index available"));
    }

    static ImmutableStructuralCallerOutput.Builder builder() {
        return ImmutableStructuralCallerOutput.builder();
    }
}
