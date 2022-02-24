package com.hartwig.pipeline.tertiary.protect;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface ProtectOutput extends StageOutput {

    @Override
    default String name() {
        return Protect.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeEvidence();

    default GoogleStorageLocation evidence() {
        return maybeEvidence().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableProtectOutput.Builder builder() {
        return ImmutableProtectOutput.builder();
    }
}
