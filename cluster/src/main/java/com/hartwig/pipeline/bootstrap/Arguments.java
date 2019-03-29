package com.hartwig.pipeline.bootstrap;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments {

    String EMPTY = "";

    enum DefaultsProfile {
        PRODUCTION,
        DEVELOPMENT
    }

    boolean forceJarUpload();

    boolean cleanup();

    boolean usePreemptibleVms();

    boolean download();

    boolean upload();

    boolean runBamMetrics();

    DefaultsProfile profile();

    String project();

    String version();

    String region();

    String jarDirectory();

    String sampleDirectory();

    String sampleId();

    String privateKeyPath();

    String sbpApiUrl();

    String sbpS3Url();

    String nodeInitializationScript();

    String cloudSdkPath();

    String referenceGenomeBucket();

    String knownIndelsBucket();

    String rclonePath();

    String rcloneGcpRemote();

    String rcloneS3Remote();

    Optional<Integer> sbpApiSampleId();

    Optional<String> runId();

    static ImmutableArguments.Builder builder() {
        return ImmutableArguments.builder();
    }

    static Arguments defaults(String profileString) {
        return defaultsBuilder(profileString).build();
    }

    static Arguments testDefaults() {
        return testDefaultsBuilder().build();
    }

    static ImmutableArguments.Builder testDefaultsBuilder() {
        return defaultsBuilder(DefaultsProfile.DEVELOPMENT.name());
    }

    static ImmutableArguments.Builder defaultsBuilder(String profileString) {
        DefaultsProfile profile = DefaultsProfile.valueOf(profileString.toUpperCase());
        if (profile.equals(DefaultsProfile.PRODUCTION)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .rclonePath(DEFAULT_PRODUCTION_RCLONE_PATH)
                    .rcloneGcpRemote(DEFAULT_PRODUCTION_RCLONE_GCP_REMOTE)
                    .rcloneS3Remote(DEFAULT_PRODUCTION_RCLONE_S3_REMOTE)
                    .region(DEFAULT_PRODUCTION_REGION)
                    .project(DEFAULT_PRODUCTION_PROJECT)
                    .version(DEFAULT_PRODUCTION_VERSION)
                    .sampleDirectory(DEFAULT_PRODUCTION_SAMPLE_DIRECTORY)
                    .nodeInitializationScript(DEFAULT_PRODUCTION_NODE_INIT)
                    .sbpApiUrl(DEFAULT_PRODUCTION_SBP_API_URL)
                    .sbpS3Url(DEFAULT_PRODUCTION_SBP_S3_URL)
                    .referenceGenomeBucket(DEFAULT_PRODUCTION_REFERENCE_GENOME_BUCKET)
                    .knownIndelsBucket(DEFAULT_PRODUCTION_KNOWN_INDELS_BUCKET)
                    .jarDirectory(DEFAULT_PRODUCTION_JAR_LIB)
                    .privateKeyPath(DEFAULT_PRODUCTION__KEY_PATH)
                    .cloudSdkPath(DEFAULT_PRODUCTION_CLOUD_SDK_PATH)
                    .forceJarUpload(false).cleanup(true).usePreemptibleVms(true).download(true).upload(true)
                    .runBamMetrics(false)
                    .sampleId(EMPTY);
        } else {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .region(DEFAULT_DEVELOPMENT_REGION)
                    .project(DEFAULT_DEVELOPMENT_PROJECT)
                    .version(DEFAULT_DEVELOPMENT_VERSION)
                    .sampleDirectory(DEFAULT_DEVELOPMENT_SAMPLE_DIRECTORY)
                    .nodeInitializationScript(DEFAULT_DEVELOPMENT_NODE_INIT)
                    .referenceGenomeBucket(DEFAULT_DEVELOPMENT_REFERENCE_GENOME_BUCKET)
                    .knownIndelsBucket(DEFAULT_DEVELOPMENT_KNOWN_INDELS_BUCKET)
                    .jarDirectory(DEFAULT_DEVELOPMENT_JAR_LIB)
                    .privateKeyPath(DEFAULT_DEVELOPMENT_KEY_PATH)
                    .cloudSdkPath(DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH)
                    .forceJarUpload(true).cleanup(false).usePreemptibleVms(true).download(false).upload(true)
                    .runBamMetrics(false)
                    .rclonePath(NOT_APPLICABLE)
                    .rcloneS3Remote(NOT_APPLICABLE)
                    .rcloneGcpRemote(NOT_APPLICABLE)
                    .sbpS3Url(NOT_APPLICABLE)
                    .sbpApiUrl(NOT_APPLICABLE)
                    .sampleId(EMPTY);
        }
    }

    static String workingDir() {
        return System.getProperty("user.dir");
    }

    String DEFAULT_PRODUCTION_RCLONE_PATH = "/usr/bin";
    String DEFAULT_PRODUCTION_RCLONE_GCP_REMOTE = "gs";
    String DEFAULT_PRODUCTION_RCLONE_S3_REMOTE = "s3";
    String DEFAULT_PRODUCTION_REGION = "europe-west4";
    String DEFAULT_PRODUCTION_PROJECT = "hmf-pipeline-production";
    String DEFAULT_PRODUCTION_VERSION = "";
    String DEFAULT_PRODUCTION_SAMPLE_DIRECTORY = "/samples";
    String DEFAULT_PRODUCTION_NODE_INIT = "node-init.sh";
    String DEFAULT_PRODUCTION_SBP_API_URL = "http://hmfapi";
    String DEFAULT_PRODUCTION_SBP_S3_URL = "https://s3.object02.schubergphilis.com";
    String DEFAULT_PRODUCTION_REFERENCE_GENOME_BUCKET = "reference_genome";
    String DEFAULT_PRODUCTION_JAR_LIB = "/usr/share/pipeline5";
    String DEFAULT_PRODUCTION__KEY_PATH = "/secrets/bootstrap-key.json";
    String DEFAULT_PRODUCTION_KNOWN_INDELS_BUCKET = "known_indels";
    String DEFAULT_PRODUCTION_CLOUD_SDK_PATH = "/usr/lib/google-cloud-sdk/bin";

    String NOT_APPLICABLE = "N/A";
    String DEFAULT_DEVELOPMENT_REGION = "europe-west4";
    String DEFAULT_DEVELOPMENT_PROJECT = "hmf-pipeline-development";
    String DEFAULT_DEVELOPMENT_VERSION = "local-SNAPSHOT";
    String DEFAULT_DEVELOPMENT_SAMPLE_DIRECTORY = workingDir() + "/samples";
    String DEFAULT_DEVELOPMENT_NODE_INIT = workingDir() + "/cluster/src/main/resources/node-init.sh";
    String DEFAULT_DEVELOPMENT_REFERENCE_GENOME_BUCKET = "reference_genome";
    String DEFAULT_DEVELOPMENT_JAR_LIB = workingDir() + "/system/target";
    String DEFAULT_DEVELOPMENT_KEY_PATH = workingDir() + "/bootstrap-key.json";
    String DEFAULT_DEVELOPMENT_KNOWN_INDELS_BUCKET = "known_indels";
    String DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH = System.getProperty("user.home") + "/gcloud/google-cloud-sdk/bin";
}
