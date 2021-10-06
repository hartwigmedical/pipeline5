package com.hartwig.pipeline.tertiary.protect;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface ProtectOutput extends StageOutput {

    @Override
    default String name() {
        return Protect.NAMESPACE;
    }

    GoogleStorageLocation evidenceTsv();

    static ImmutableProtectOutput.Builder builder() {
        return ImmutableProtectOutput.builder();
    }
}
