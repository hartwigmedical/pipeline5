package com.hartwig.pipeline;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments extends CommonArguments {

    String EMPTY = "";

    enum DefaultsProfile {
        PRODUCTION,
        DEVELOPMENT,
        DEVELOPMENT_DOCKER
    }

    boolean cleanup();

    Integer DEFAULT_POLL_INTERVAL = 5;

    boolean upload();

    boolean uploadFromGcp();

    boolean runBamMetrics();

    boolean runAligner();

    boolean runSnpGenotyper();

    boolean runGermlineCaller();

    boolean runSomaticCaller();

    boolean runSageCaller();

    boolean runStructuralCaller();

    boolean runTertiary();

    boolean shallow();

    DefaultsProfile profile();

    String version();

    String sampleDirectory();

    String sampleId();

    String setId();

    String sbpApiUrl();

    String sbpS3Url();

    String rclonePath();

    String rcloneGcpRemote();

    String rcloneS3RemoteDownload();

    String rcloneS3RemoteUpload();

    String patientReportBucket();

    String archiveBucket();

    String archiveProject();

    String archivePrivateKeyPath();

    Optional<Integer> sbpApiSampleId();

    Optional<Integer> sbpApiRunId();

    Optional<String> runId();

    Optional<String> zone();

    int maxConcurrentLanes();

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
        return defaultsBuilder(DefaultsProfile.DEVELOPMENT.name()).runId("test");
    }

    boolean outputCram();

    static String workingDir() {
        return System.getProperty("user.dir");
    }

    String DEFAULT_PRODUCTION_RCLONE_PATH = "/usr/bin";
    String DEFAULT_PRODUCTION_RCLONE_GCP_REMOTE = "gs";
    String DEFAULT_PRODUCTION_RCLONE_S3_REMOTE = "s3";
    String DEFAULT_PRODUCTION_REGION = "europe-west4";
    String DEFAULT_PRODUCTION_PROJECT = "hmf-pipeline-prod-e45b00f2";
    String DEFAULT_PRODUCTION_VERSION = "";
    String DEFAULT_PRODUCTION_SBP_API_URL = "http://hmfapi";
    String DEFAULT_PRODUCTION_SBP_S3_URL = "https://s3.object02.schubergphilis.com";
    String DEFAULT_PRODUCTION_SERVICE_ACCOUNT_EMAIL = String.format("bootstrap@%s.iam.gserviceaccount.com", DEFAULT_PRODUCTION_PROJECT);
    String DEFAULT_PRODUCTION_PATIENT_REPORT_BUCKET = "pipeline-output-prod";
    String DEFAULT_PRODUCTION_ARCHIVE_BUCKET = "pipeline-archive-prod";
    String DEFAULT_PRODUCTION_ARCHIVE_PROJECT = DEFAULT_PRODUCTION_PROJECT;

    String DEFAULT_DOCKER_SAMPLE_DIRECTORY = "/samples";
    String DEFAULT_DOCKER_KEY_PATH = "/secrets/bootstrap-key.json";
    String DEFAULT_DOCKER_ARCHIVE_KEY_PATH = "/secrets/archive-key.json";
    String DEFAULT_DOCKER_CLOUD_SDK_PATH = "/usr/lib/google-cloud-sdk/bin";

    String NOT_APPLICABLE = "N/A";
    String DEFAULT_DEVELOPMENT_REGION = "europe-west4";
    String DEFAULT_DEVELOPMENT_PROJECT = "hmf-pipeline-development";
    String DEFAULT_DEVELOPMENT_VERSION = "local-SNAPSHOT";
    String DEFAULT_DEVELOPMENT_SAMPLE_DIRECTORY = workingDir() + "/samples";
    String DEFAULT_DEVELOPMENT_KEY_PATH = workingDir() + "/bootstrap-key.json";
    String DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH = System.getProperty("user.home") + "/gcloud/google-cloud-sdk/bin";
    String DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL = String.format("bootstrap@%s.iam.gserviceaccount.com", DEFAULT_DEVELOPMENT_PROJECT);
    String DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET = "pipeline-output-dev";
    String DEFAULT_DEVELOPMENT_ARCHIVE_BUCKET = "pipeline-archive-dev";

    int DEFAULT_MAX_CONCURRENT_LANES = 8;

    static ImmutableArguments.Builder defaultsBuilder(String profileString) {
        DefaultsProfile profile = DefaultsProfile.valueOf(profileString.toUpperCase());
        if (profile.equals(DefaultsProfile.PRODUCTION)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .rclonePath(DEFAULT_PRODUCTION_RCLONE_PATH)
                    .rcloneGcpRemote(DEFAULT_PRODUCTION_RCLONE_GCP_REMOTE)
                    .rcloneS3RemoteDownload(DEFAULT_PRODUCTION_RCLONE_S3_REMOTE)
                    .rcloneS3RemoteUpload(DEFAULT_PRODUCTION_RCLONE_S3_REMOTE)
                    .region(DEFAULT_PRODUCTION_REGION)
                    .project(DEFAULT_PRODUCTION_PROJECT)
                    .version(DEFAULT_PRODUCTION_VERSION)
                    .sampleDirectory(DEFAULT_DOCKER_SAMPLE_DIRECTORY)
                    .sbpApiUrl(DEFAULT_PRODUCTION_SBP_API_URL)
                    .sbpS3Url(DEFAULT_PRODUCTION_SBP_S3_URL)
                    .privateKeyPath(DEFAULT_DOCKER_KEY_PATH)
                    .serviceAccountEmail(DEFAULT_PRODUCTION_SERVICE_ACCOUNT_EMAIL)
                    .cloudSdkPath(DEFAULT_DOCKER_CLOUD_SDK_PATH)
                    .cleanup(true)
                    .usePreemptibleVms(true)
                    .useLocalSsds(true)
                    .upload(true)
                    .uploadFromGcp(false)
                    .runBamMetrics(true)
                    .runAligner(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runSomaticCaller(true)
                    .runSageCaller(true)
                    .runStructuralCaller(true)
                    .runTertiary(true)
                    .shallow(false)
                    .sampleId(EMPTY)
                    .setId(EMPTY)
                    .patientReportBucket(DEFAULT_PRODUCTION_PATIENT_REPORT_BUCKET)
                    .archiveBucket(DEFAULT_PRODUCTION_ARCHIVE_BUCKET)
                    .archiveProject(DEFAULT_PRODUCTION_ARCHIVE_PROJECT)
                    .archivePrivateKeyPath(DEFAULT_DOCKER_ARCHIVE_KEY_PATH)
                    .outputCram(false)
                    .pollInterval(DEFAULT_POLL_INTERVAL)
                    .maxConcurrentLanes(DEFAULT_MAX_CONCURRENT_LANES);
        } else if (profile.equals(DefaultsProfile.DEVELOPMENT)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .region(DEFAULT_DEVELOPMENT_REGION)
                    .project(DEFAULT_DEVELOPMENT_PROJECT)
                    .version(DEFAULT_DEVELOPMENT_VERSION)
                    .sampleDirectory(DEFAULT_DEVELOPMENT_SAMPLE_DIRECTORY)
                    .cloudSdkPath(DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH)
                    .serviceAccountEmail(DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL)
                    .cleanup(true)
                    .usePreemptibleVms(true)
                    .useLocalSsds(true)
                    .upload(true)
                    .uploadFromGcp(false)
                    .runBamMetrics(true)
                    .runAligner(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runSomaticCaller(true)
                    .runSageCaller(true)
                    .runTertiary(true)
                    .runStructuralCaller(true)
                    .shallow(false)
                    .rclonePath(NOT_APPLICABLE)
                    .rcloneS3RemoteDownload(NOT_APPLICABLE)
                    .rcloneS3RemoteUpload(NOT_APPLICABLE)
                    .rcloneGcpRemote(NOT_APPLICABLE)
                    .sbpS3Url(EMPTY)
                    .sbpApiUrl(NOT_APPLICABLE)
                    .sampleId(EMPTY)
                    .setId(EMPTY)
                    .patientReportBucket(DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET)
                    .archiveBucket(DEFAULT_DEVELOPMENT_ARCHIVE_BUCKET)
                    .archiveProject(DEFAULT_DEVELOPMENT_PROJECT)
                    .archivePrivateKeyPath(DEFAULT_DEVELOPMENT_KEY_PATH)
                    .outputCram(false)
                    .pollInterval(DEFAULT_POLL_INTERVAL)
                    .maxConcurrentLanes(DEFAULT_MAX_CONCURRENT_LANES);
        } else if (profile.equals(DefaultsProfile.DEVELOPMENT_DOCKER)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .region(DEFAULT_DEVELOPMENT_REGION)
                    .project(DEFAULT_DEVELOPMENT_PROJECT)
                    .version(DEFAULT_DEVELOPMENT_VERSION)
                    .sampleDirectory(DEFAULT_DOCKER_SAMPLE_DIRECTORY)
                    .cloudSdkPath(DEFAULT_DOCKER_CLOUD_SDK_PATH)
                    .serviceAccountEmail(DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL)
                    .cleanup(true)
                    .usePreemptibleVms(true)
                    .useLocalSsds(true)
                    .upload(true)
                    .uploadFromGcp(false)
                    .runBamMetrics(true)
                    .runAligner(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runSomaticCaller(true)
                    .runSageCaller(true)
                    .runTertiary(true)
                    .runStructuralCaller(true)
                    .shallow(false)
                    .rclonePath(NOT_APPLICABLE)
                    .rcloneS3RemoteDownload(NOT_APPLICABLE)
                    .rcloneS3RemoteUpload(NOT_APPLICABLE)
                    .rcloneGcpRemote(NOT_APPLICABLE)
                    .sbpS3Url(EMPTY)
                    .sbpApiUrl(NOT_APPLICABLE)
                    .sampleId(EMPTY)
                    .setId(EMPTY)
                    .patientReportBucket(DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET)
                    .archiveBucket(DEFAULT_DEVELOPMENT_ARCHIVE_BUCKET)
                    .archiveProject(DEFAULT_DEVELOPMENT_PROJECT)
                    .archivePrivateKeyPath(DEFAULT_DOCKER_KEY_PATH)
                    .outputCram(false)
                    .pollInterval(DEFAULT_POLL_INTERVAL)
                    .maxConcurrentLanes(DEFAULT_MAX_CONCURRENT_LANES);
        }
        throw new IllegalArgumentException(String.format("Unknown profile [%s], please create defaults for this profile.", profile));
    }
}
