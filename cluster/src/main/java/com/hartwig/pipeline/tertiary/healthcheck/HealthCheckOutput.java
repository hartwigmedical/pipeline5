package com.hartwig.pipeline.tertiary.healthcheck;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface HealthCheckOutput extends StageOutput {

    @Override
    default String name() {
        return HealthChecker.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputFile();

    default GoogleStorageLocation outputFile() {
        return maybeOutputFile().orElseThrow(() -> new IllegalStateException("No output file available"));
    }

    static ImmutableHealthCheckOutput.Builder builder() {
        return ImmutableHealthCheckOutput.builder();
    }
}
