package com.hartwig.pipeline.tertiary.healthcheck;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface HealthCheckOutput {

    JobStatus status();

    GoogleStorageLocation outputFile();

    static ImmutableHealthCheckOutput.Builder builder() {
        return ImmutableHealthCheckOutput.builder();
    }
}
