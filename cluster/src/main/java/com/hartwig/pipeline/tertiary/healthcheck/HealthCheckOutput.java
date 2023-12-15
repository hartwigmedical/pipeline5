package com.hartwig.pipeline.tertiary.healthcheck;

import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;

import org.immutables.value.Value;

@Value.Immutable
public interface HealthCheckOutput extends StageOutput {

    @Override
    default String name() {
        return HealthChecker.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeOutputDirectory();

    default GoogleStorageLocation outputDirectory() {
        return maybeOutputDirectory().orElseThrow(() -> new IllegalStateException("No output directory available"));
    }

    static ImmutableHealthCheckOutput.Builder builder() {
        return ImmutableHealthCheckOutput.builder();
    }
}
