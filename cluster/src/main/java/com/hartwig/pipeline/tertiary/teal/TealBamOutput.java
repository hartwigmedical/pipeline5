package com.hartwig.pipeline.tertiary.teal;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface TealBamOutput extends StageOutput {
    @Override
    default String name() {
        return TealBam.NAMESPACE;
    }

    Optional<GoogleStorageLocation> germlineTelbam();

    Optional<GoogleStorageLocation> somaticTelbam();

    static ImmutableTealBamOutput.Builder builder() {
        return ImmutableTealBamOutput.builder();
    }
}
