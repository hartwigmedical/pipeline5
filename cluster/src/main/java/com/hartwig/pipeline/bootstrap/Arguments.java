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

    @Value.Default
    default boolean noPreemptibleVms() {
        return false;
    }

    @Value.Default
    default boolean noDownload() {
        return false;
    }

    @Value.Default
    default boolean verboseCloudSdk() {
        return false;
    }

    @Value.Default
    default boolean noUpload() {
        return false;
    }

    @Value.Default
    default boolean runBamMetrics() { return false; }

    @Value.Default
    default boolean useRclone() {
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

    String nodeInitializationScript();

    String cloudSdkPath();

    int cpuPerGBRatio();

    String referenceGenomeBucket();

    String knownIndelsBucket();

    int s3UploadThreads();

    int cloudSdkTimeoutHours();

    String rclonePath();

    String rcloneGcpRemote();

    String rcloneS3Remote();

    String clusterIdleTtl();

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
