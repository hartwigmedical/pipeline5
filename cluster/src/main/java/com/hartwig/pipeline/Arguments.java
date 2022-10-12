package com.hartwig.pipeline;

import java.util.Optional;

import com.hartwig.events.Pipeline;
import com.hartwig.events.Pipeline.Context;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.resource.RefGenomeVersion;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments extends CommonArguments {

    String EMPTY = "";

    Optional<String> startingPoint();

    boolean publishDbLoadEvent();

    enum DefaultsProfile {
        PUBLIC,
        PRODUCTION,
        DEVELOPMENT,
        DEVELOPMENT_DOCKER
    }

    boolean cleanup();

    Integer DEFAULT_POLL_INTERVAL = 5;

    boolean runBamMetrics();

    boolean runSnpGenotyper();

    boolean runGermlineCaller();

    boolean runTertiary();

    boolean shallow();

    DefaultsProfile profile();

    String setId();

    Optional<String> biopsy();

    String sbpApiUrl();

    String outputBucket();

    String uploadPrivateKeyPath();

    Optional<Integer> sbpApiRunId();

    Optional<String> runId();

    Optional<String> zone();

    Optional<String> sampleJson();

    int maxConcurrentLanes();

    static ImmutableArguments.Builder builder() {
        return ImmutableArguments.builder();
    }

    static Arguments defaults(final String profileString) {
        return defaultsBuilder(profileString).build();
    }

    static Arguments testDefaults() {
        return testDefaultsBuilder().build();
    }

    static ImmutableArguments.Builder testDefaultsBuilder() {
        return defaultsBuilder(DefaultsProfile.DEVELOPMENT.name()).runId("test");
    }

    boolean outputCram();

    boolean publishToTurquoise();

    boolean useCrams();

    boolean anonymize();

    Pipeline.Context context();

    static String workingDir() {
        return System.getProperty("user.dir");
    }

    String DEFAULT_PRODUCTION_PROJECT = "hmf-pipeline-prod-e45b00f2";
    String DEFAULT_PRODUCTION_SBP_API_URL = "http://hmfapi";
    String DEFAULT_PRODUCTION_SERVICE_ACCOUNT_EMAIL = String.format("bootstrap@%s.iam.gserviceaccount.com", DEFAULT_PRODUCTION_PROJECT);
    String DEFAULT_PRODUCTION_PATIENT_REPORT_BUCKET = "pipeline-output-prod";

    String DEFAULT_DOCKER_KEY_PATH = "/secrets/bootstrap-key.json";
    String DEFAULT_DOCKER_UPLOAD_KEY_PATH = "/secrets/upload-key.json";
    String DEFAULT_DOCKER_CLOUD_SDK_PATH = "/usr/lib/google-cloud-sdk/bin";

    String NOT_APPLICABLE = "N/A";
    String DEFAULT_DEVELOPMENT_KEY_PATH = workingDir() + "/bootstrap-key.json";
    String DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET = "pipeline-output-dev";

    RefGenomeVersion DEFAULT_REF_GENOME_VERSION = RefGenomeVersion.V37;

    int DEFAULT_MAX_CONCURRENT_LANES = 8;

    Pipeline.Context DEFAULT_CONTEXT = Context.DIAGNOSTIC;

    static ImmutableArguments.Builder defaultsBuilder(final String profileString) {
        DefaultsProfile profile = DefaultsProfile.valueOf(profileString.toUpperCase());
        if (profile.equals(DefaultsProfile.PRODUCTION)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .region(CommonArguments.DEFAULT_REGION)
                    .project(DEFAULT_PRODUCTION_PROJECT)
                    .sbpApiUrl(DEFAULT_PRODUCTION_SBP_API_URL)
                    .privateKeyPath(DEFAULT_DOCKER_KEY_PATH)
                    .serviceAccountEmail(DEFAULT_PRODUCTION_SERVICE_ACCOUNT_EMAIL)
                    .cloudSdkPath(DEFAULT_DOCKER_CLOUD_SDK_PATH)
                    .cleanup(true)
                    .usePreemptibleVms(true)
                    .useLocalSsds(true)
                    .runBamMetrics(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runTertiary(true)
                    .shallow(false)
                    .setId(EMPTY)
                    .cmek(EMPTY)
                    .outputBucket(DEFAULT_PRODUCTION_PATIENT_REPORT_BUCKET)
                    .uploadPrivateKeyPath(DEFAULT_DOCKER_KEY_PATH)
                    .network(DEFAULT_NETWORK)
                    .outputCram(true)
                    .publishToTurquoise(false)
                    .publishDbLoadEvent(false)
                    .pollInterval(DEFAULT_POLL_INTERVAL)
                    .refGenomeVersion(DEFAULT_REF_GENOME_VERSION)
                    .maxConcurrentLanes(DEFAULT_MAX_CONCURRENT_LANES)
                    .useCrams(false)
                    .useTargetRegions(false)
                    .anonymize(false)
                    .context(DEFAULT_CONTEXT);
        } else if (profile.equals(DefaultsProfile.DEVELOPMENT)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .region(CommonArguments.DEFAULT_DEVELOPMENT_REGION)
                    .project(CommonArguments.DEFAULT_DEVELOPMENT_PROJECT)
                    .cloudSdkPath(CommonArguments.DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH)
                    .serviceAccountEmail(CommonArguments.DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL)
                    .cleanup(true)
                    .cmek(CommonArguments.DEFAULT_DEVELOPMENT_CMEK)
                    .usePreemptibleVms(true)
                    .runBamMetrics(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runTertiary(true)
                    .shallow(false)
                    .sbpApiUrl(NOT_APPLICABLE)
                    .setId(EMPTY)
                    .outputBucket(DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET)
                    .uploadPrivateKeyPath(DEFAULT_DEVELOPMENT_KEY_PATH)
                    .outputCram(true)
                    .publishToTurquoise(false)
                    .publishDbLoadEvent(false)
                    .pollInterval(DEFAULT_POLL_INTERVAL)
                    .refGenomeVersion(DEFAULT_REF_GENOME_VERSION)
                    .maxConcurrentLanes(DEFAULT_MAX_CONCURRENT_LANES)
                    .network(DEFAULT_NETWORK)
                    .useLocalSsds(true)
                    .useCrams(false)
                    .useTargetRegions(false)
                    .anonymize(false)
                    .context(DEFAULT_CONTEXT)
                    .userLabel(System.getProperty("user.name"));
        } else if (profile.equals(DefaultsProfile.DEVELOPMENT_DOCKER)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .region(CommonArguments.DEFAULT_DEVELOPMENT_REGION)
                    .project(CommonArguments.DEFAULT_DEVELOPMENT_PROJECT)
                    .cloudSdkPath(DEFAULT_DOCKER_CLOUD_SDK_PATH)
                    .serviceAccountEmail(CommonArguments.DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL)
                    .cleanup(true)
                    .cmek(DEFAULT_DEVELOPMENT_CMEK)
                    .usePreemptibleVms(true)
                    .useLocalSsds(true)
                    .runBamMetrics(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runTertiary(true)
                    .shallow(false)
                    .sbpApiUrl(NOT_APPLICABLE)
                    .setId(EMPTY)
                    .outputBucket(DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET)
                    .uploadPrivateKeyPath(DEFAULT_DOCKER_UPLOAD_KEY_PATH)
                    .network(DEFAULT_NETWORK)
                    .outputCram(true)
                    .publishToTurquoise(false)
                    .publishDbLoadEvent(false)
                    .pollInterval(DEFAULT_POLL_INTERVAL)
                    .refGenomeVersion(DEFAULT_REF_GENOME_VERSION)
                    .maxConcurrentLanes(DEFAULT_MAX_CONCURRENT_LANES)
                    .useCrams(false)
                    .useTargetRegions(false)
                    .anonymize(false)
                    .context(DEFAULT_CONTEXT);
        } else if (profile.equals(DefaultsProfile.PUBLIC)) {
            return ImmutableArguments.builder()
                    .profile(profile)
                    .outputBucket(EMPTY)
                    .region(EMPTY)
                    .project(EMPTY)
                    .serviceAccountEmail(EMPTY)
                    .cloudSdkPath(DEFAULT_DOCKER_CLOUD_SDK_PATH)
                    .cleanup(true)
                    .usePreemptibleVms(true)
                    .useLocalSsds(true)
                    .runBamMetrics(true)
                    .runSnpGenotyper(true)
                    .runGermlineCaller(true)
                    .runTertiary(true)
                    .shallow(false)
                    .sbpApiUrl(NOT_APPLICABLE)
                    .setId(EMPTY)
                    .uploadPrivateKeyPath(DEFAULT_DOCKER_UPLOAD_KEY_PATH)
                    .network(DEFAULT_NETWORK)
                    .outputCram(true)
                    .publishToTurquoise(false)
                    .publishDbLoadEvent(false)
                    .pollInterval(DEFAULT_POLL_INTERVAL)
                    .refGenomeVersion(RefGenomeVersion.V38)
                    .maxConcurrentLanes(DEFAULT_MAX_CONCURRENT_LANES)
                    .imageName(VirtualMachineJobDefinition.PUBLIC_IMAGE_NAME)
                    .useCrams(false)
                    .useTargetRegions(false)
                    .anonymize(false)
                    .context(DEFAULT_CONTEXT);
        }
        throw new IllegalArgumentException(String.format("Unknown profile [%s], please create defaults for this profile.", profile));
    }
}
