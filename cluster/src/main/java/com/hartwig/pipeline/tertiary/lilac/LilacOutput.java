package com.hartwig.pipeline.tertiary.lilac;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.computeengine.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface LilacOutput extends StageOutput {
    @Override
    default String name() {
        return Lilac.NAMESPACE;
    }

    Optional<GoogleStorageLocation> result();

    Optional<GoogleStorageLocation> qc();

    static ImmutableLilacOutput.Builder builder() {
        return ImmutableLilacOutput.builder();
    }
}
