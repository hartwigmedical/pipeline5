package com.hartwig.pipeline.bootstrap;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments {

    @Value.Default
    default boolean skipPatientUpload() {
        return false;
    }

    @Value.Default
    default boolean forceJarUpload() {
        return false;
    }

    String runtimeBucket();

    String project();

    String version();

    String region();

    String jarLibDirectory();

    String patientId();

    String patientDirectory();

    String privateKeyPath();

    String sbpApiUrl();

    Optional<Integer> sbpApiSampleId();

    static ImmutableArguments.Builder builder() {
        return ImmutableArguments.builder();
    }
}
