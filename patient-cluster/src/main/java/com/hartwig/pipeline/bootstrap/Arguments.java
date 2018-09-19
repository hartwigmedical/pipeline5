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

    Optional<String> runId();

    static Arguments defaults() {
        return BootstrapOptions.from(new String[] {}).orElseThrow(IllegalStateException::new);
    }

    static ImmutableArguments.Builder defaultsBuilder() {
        return ImmutableArguments.builder().from(BootstrapOptions.from(new String[] {}).orElseThrow(IllegalStateException::new));
    }

    static ImmutableArguments.Builder builder() {
        return ImmutableArguments.builder();
    }
}
