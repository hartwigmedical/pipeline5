package com.hartwig.pipeline.alignment.after;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface BamMetricsOutput {
    JobStatus status();

    Optional<GoogleStorageLocation> metrics();

    static ImmutableBamMetricsOutput.Builder builder() {
        return ImmutableBamMetricsOutput.builder();
    }
}
