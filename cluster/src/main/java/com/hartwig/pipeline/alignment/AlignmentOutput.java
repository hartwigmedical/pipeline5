package com.hartwig.pipeline.alignment;

import java.util.Optional;
import java.util.function.Supplier;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

@Value.Immutable
public interface AlignmentOutput extends StageOutput {

    Sample sample();

    default String name() {
        return Aligner.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeFinalBamLocation();

    Optional<GoogleStorageLocation> maybeFinalBaiLocation();

    Optional<GoogleStorageLocation> maybeRecalibratedBamLocation();

    Optional<GoogleStorageLocation> maybeRecalibratedBaiLocation();

    default GoogleStorageLocation finalBamLocation() {
        return maybeFinalBamLocation().orElseThrow(noBamAvailable());
    }

    default GoogleStorageLocation finalBaiLocation() {
        return maybeFinalBaiLocation().orElseThrow(noBamAvailable());
    }

    default GoogleStorageLocation recalibratedBamLocation() {
        return maybeRecalibratedBaiLocation().orElseThrow(noBamAvailable());
    }

    default GoogleStorageLocation recalibratedBaiLocation() {
        return maybeRecalibratedBamLocation().orElseThrow(noBamAvailable());
    }

    static Supplier<IllegalStateException> noBamAvailable() {
        return () -> new IllegalStateException("No BAM available");
    }

    static ImmutableAlignmentOutput.Builder builder() {
        return ImmutableAlignmentOutput.builder();
    }
}
