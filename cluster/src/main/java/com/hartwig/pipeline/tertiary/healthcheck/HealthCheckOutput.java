package com.hartwig.pipeline.tertiary.healthcheck;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

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
