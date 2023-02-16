package com.hartwig.pipeline.tertiary.sigs;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.sigs.ImmutableSigsOutput;

import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface SigsOutput extends StageOutput {

    static ImmutableSigsOutput.Builder builder() {
        return ImmutableSigsOutput.builder();
    }

    Optional<GoogleStorageLocation> maybeAllocationTsv();

    default GoogleStorageLocation allocationTsv() {
        return maybeAllocationTsv().orElse(GoogleStorageLocation.empty());
    }

    @Override
    default String name() {
        return Sigs.NAMESPACE;
    }
}
