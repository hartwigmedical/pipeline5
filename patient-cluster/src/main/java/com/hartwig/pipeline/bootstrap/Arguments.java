package com.hartwig.pipeline.bootstrap;

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

    static ImmutableArguments.Builder builder() {
        return ImmutableArguments.builder();
    }
}
