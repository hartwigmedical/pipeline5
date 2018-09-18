package com.hartwig.pipeline.bootstrap;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments {

    @Value.Default
    default boolean forceJarUpload() {
        return false;
    }

    @Value.Default
    default boolean noCleanup() {
        return false;
    }

    String project();

    String version();

    String region();

    String jarLibDirectory();

    String patientId();

    String patientDirectory();

    String privateKeyPath();

    String sbpApiUrl();

    String sblS3Url();

    Optional<Integer> sbpApiSampleId();

    static ImmutableArguments.Builder builder() {
        return ImmutableArguments.builder();
    }
}
