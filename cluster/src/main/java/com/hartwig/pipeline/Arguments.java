package com.hartwig.pipeline;

import java.util.Optional;

import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.pipeline.resource.RefGenomeVersion;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments extends CommonArguments {

    String EMPTY = "";
    String DEFAULT_SAMPLE_JSON = "sample.json";
    Integer DEFAULT_POLL_INTERVAL = 5;
    String DEFAULT_PRODUCTION_PROJECT = "hmf-pipeline-prod-e45b00f2";
    String DEFAULT_PRODUCTION_SERVICE_ACCOUNT_EMAIL = String.format("bootstrap@%s.iam.gserviceaccount.com", DEFAULT_PRODUCTION_PROJECT);
    String DEFAULT_PRODUCTION_PATIENT_REPORT_BUCKET = "pipeline-output-prod";
    String DEFAULT_DOCKER_CLOUD_SDK_PATH = "/usr/lib/google-cloud-sdk/bin";
    String DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET = "pipeline-output-dev";
    String VIRTUAL_MACHINE_PUBLIC_IMAGE_NAME = "hmf-public-pipeline-v1";
    RefGenomeVersion DEFAULT_REF_GENOME_VERSION = RefGenomeVersion.V37;
    int DEFAULT_MAX_CONCURRENT_LANES = 8;
    Pipeline.Context DEFAULT_CONTEXT = Pipeline.Context.DIAGNOSTIC;

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
        return defaultsBuilder(DefaultsProfile.DEVELOPMENT.name()).runTag("test");
    }

    static ImmutableArguments.Builder defaultsBuilder(final String profileString) {
        ImmutableArguments.Builder builder = ImmutableArguments.builder()
                .cleanup(true)
                .usePreemptibleVms(true)
                .useLocalSsds(true)
                .vmSelfDeleteOnShutdown(true)
                .redoDuplicateMarking(false)
                .runBamMetrics(true)
                .runSnpGenotyper(true)
                .runGermlineCaller(true)
                .runTertiary(true)
                .shallow(false)
                .setName(EMPTY)
                .network(DEFAULT_NETWORK)
                .outputCram(true)
                .publishToTurquoise(false)
                .publishDbLoadEvent(false)
                .publishEventsOnly(false)
                .pollInterval(DEFAULT_POLL_INTERVAL)
                .refGenomeVersion(DEFAULT_REF_GENOME_VERSION)
                .maxConcurrentLanes(DEFAULT_MAX_CONCURRENT_LANES)
                .useCrams(false)
                .useTargetRegions(false)
                .anonymize(false)
                .usePrivateResources(false)
                .context(DEFAULT_CONTEXT)
                .sampleJson(DEFAULT_SAMPLE_JSON);

        DefaultsProfile profile = DefaultsProfile.valueOf(profileString.toUpperCase());
        if (profile.equals(DefaultsProfile.PRODUCTION)) {
            return builder
                    .profile(profile)
                    .region(CommonArguments.DEFAULT_REGION)
                    .project(DEFAULT_PRODUCTION_PROJECT)
                    .serviceAccountEmail(DEFAULT_PRODUCTION_SERVICE_ACCOUNT_EMAIL)
                    .cloudSdkPath(DEFAULT_DOCKER_CLOUD_SDK_PATH)
                    .outputBucket(DEFAULT_PRODUCTION_PATIENT_REPORT_BUCKET);
        } else if (profile.equals(DefaultsProfile.DEVELOPMENT)) {
            return builder
                    .profile(profile)
                    .region(CommonArguments.DEFAULT_DEVELOPMENT_REGION)
                    .project(CommonArguments.DEFAULT_DEVELOPMENT_PROJECT)
                    .cloudSdkPath(CommonArguments.DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH)
                    .serviceAccountEmail(CommonArguments.DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL)
                    .cmek(CommonArguments.DEFAULT_DEVELOPMENT_CMEK)
                    .outputBucket(DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET)
                    .userLabel(System.getProperty("user.name"));
        } else if (profile.equals(DefaultsProfile.DEVELOPMENT_DOCKER)) {
            return builder
                    .profile(profile)
                    .region(CommonArguments.DEFAULT_DEVELOPMENT_REGION)
                    .project(CommonArguments.DEFAULT_DEVELOPMENT_PROJECT)
                    .cloudSdkPath(DEFAULT_DOCKER_CLOUD_SDK_PATH)
                    .serviceAccountEmail(CommonArguments.DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL)
                    .cmek(DEFAULT_DEVELOPMENT_CMEK)
                    .outputBucket(DEFAULT_DEVELOPMENT_PATIENT_REPORT_BUCKET);
        } else if (profile.equals(DefaultsProfile.PUBLIC)) {
            return builder
                    .profile(profile)
                    .outputBucket(EMPTY)
                    .region(EMPTY)
                    .project(EMPTY)
                    .serviceAccountEmail(EMPTY)
                    .cloudSdkPath(DEFAULT_DOCKER_CLOUD_SDK_PATH)
                    .refGenomeVersion(RefGenomeVersion.V38)
                    .imageName(VIRTUAL_MACHINE_PUBLIC_IMAGE_NAME);
        }
        throw new IllegalArgumentException(String.format("Unknown profile [%s], please create defaults for this profile.", profile));
    }

    Optional<String> startingPoint();

    boolean publishDbLoadEvent();

    boolean cleanup();

    boolean redoDuplicateMarking();

    boolean runBamMetrics();

    boolean runSnpGenotyper();

    boolean runGermlineCaller();

    boolean runTertiary();

    boolean shallow();

    boolean useTargetRegions();

    DefaultsProfile profile();

    String setName();

    Optional<String> biopsy();

    Optional<String> hmfApiUrl();

    String outputBucket();

    Optional<String> runTag();

    String sampleJson();

    int maxConcurrentLanes();

    boolean outputCram();

    boolean publishToTurquoise();

    boolean useCrams();

    boolean anonymize();

    boolean usePrivateResources();

    Pipeline.Context context();

    boolean publishEventsOnly();

    enum DefaultsProfile {
        PUBLIC,
        PRODUCTION,
        DEVELOPMENT,
        DEVELOPMENT_DOCKER
    }
}
