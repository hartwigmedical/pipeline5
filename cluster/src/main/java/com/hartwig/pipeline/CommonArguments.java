package com.hartwig.pipeline;

import java.util.List;
import java.util.Optional;

import com.hartwig.pipeline.resource.RefGenomeVersion;

import org.apache.commons.cli.CommandLine;

public interface CommonArguments {

    String PRIVATE_KEY_PATH = "private_key_path";
    String POLL_INTERVAL = "poll_interval";
    String DEFAULT_NETWORK = "default";
    String DEFAULT_DEVELOPMENT_CMEK =
            "projects/hmf-pipeline-development/locations/europe-west4/keyRings" + "/hmf-pipeline-development/cryptoKeys/default-test";
    String DEFAULT_DEVELOPMENT_REGION = "europe-west4";
    String DEFAULT_DEVELOPMENT_PROJECT = "hmf-pipeline-development";
    String DEFAULT_DEVELOPMENT_CLOUD_SDK_PATH = System.getProperty("user.home") + "/gcloud/google-cloud-sdk/bin";
    String DEFAULT_DEVELOPMENT_SERVICE_ACCOUNT_EMAIL = String.format("bootstrap@%s.iam.gserviceaccount.com", DEFAULT_DEVELOPMENT_PROJECT);
    String DEFAULT_REGION = "europe-west4";

    String project();

    Optional<String> privateKeyPath();

    String cloudSdkPath();

    String region();

    boolean usePreemptibleVms();

    boolean useLocalSsds();

    String network();

    Optional<String> subnet();

    List<String> tags();

    Integer pollInterval();

    String serviceAccountEmail();

    Optional<String> cmek();

    Optional<String> runId();

    Optional<Integer> sbpApiRunId();

    Optional<String> imageName();

    Optional<String> imageProject();

    RefGenomeVersion refGenomeVersion();

    Optional<String> refGenomeUrl();

    Optional<String> pubsubProject();

    Optional<String> pubsubTopicWorkflow();

    Optional<String> costCenterLabel();

    Optional<String> userLabel();

    boolean useTargetRegions();

    static Optional<String> privateKey(final CommandLine commandLine) {
        if (commandLine.hasOption(PRIVATE_KEY_PATH)) {
            return Optional.of(commandLine.getOptionValue(PRIVATE_KEY_PATH));
        }
        return Optional.empty();
    }
}
