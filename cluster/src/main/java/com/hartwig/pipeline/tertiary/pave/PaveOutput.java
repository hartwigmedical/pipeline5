package com.hartwig.pipeline.tertiary.pave;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface PaveOutput extends StageOutput {

    Optional<GoogleStorageLocation> maybeFinalVcf();

    default GoogleStorageLocation finalVcf() {
        return maybeFinalVcf().orElseThrow(() -> new IllegalStateException("No final vcf available"));
    }

    static ImmutablePaveOutput.Builder builder() {
        return ImmutablePaveOutput.builder();
    }
}
