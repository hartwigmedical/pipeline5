package com.hartwig.pipeline.alignment;

import java.util.Optional;
import java.util.function.Supplier;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface AlignmentOutput extends StageOutput {

    Sample sample();

    default String name() {
        return Aligner.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeFinalBamLocation();

    Optional<GoogleStorageLocation> maybeFinalBaiLocation();

    default GoogleStorageLocation finalBamLocation() {
        return maybeFinalBamLocation().orElseThrow(noBamAvailable());
    }

    default GoogleStorageLocation finalBaiLocation() {
        return maybeFinalBaiLocation().orElseThrow(noBaiAvailable());
    }

    static Supplier<IllegalStateException> noBamAvailable() {
        return () -> new IllegalStateException("No BAM available");
    }

    static Supplier<IllegalStateException> noBaiAvailable() {
        return () -> new IllegalStateException("No BAI available");
    }

    static ImmutableAlignmentOutput.Builder builder() {
        return ImmutableAlignmentOutput.builder();
    }
}
