package com.hartwig.pipeline;

import java.util.List;
import java.util.Optional;

import com.hartwig.pipeline.resource.RefGenomeVersion;

import org.apache.commons.cli.CommandLine;

public interface CommonArguments {

    String PROJECT = "project";
    String REGION = "region";
    String LOCAL_SSDS = "local_ssds";
    String POLL_INTERVAL = "poll_interval";
    String PREEMPTIBLE_VMS = "preemptible_vms";
    String SERVICE_ACCOUNT_EMAIL = "service_account_email";
    String CLOUD_SDK = "cloud_sdk";
    String PRIVATE_KEY_PATH = "private_key_path";
    String CMEK = "cmek";
    String PRIVATE_NETWORK = "network";
    String SUBNET = "subnet";

    String CMEK_DESCRIPTION = "The resource path of the Customer Managed Encryption Key. Runtime buckets will use this key.";
    String DEFAULT_NETWORK = "default";

    String PRIVATE_NETWORK_DESCRIPTION = "The name of the private network to use. Specifying a value here will use this "
            + "network and subnet of the same name and disable external IPs. Ensure the network has been created in GCP before enabling "
            + "this flag";
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

    static Optional<String> privateKey(CommandLine commandLine) {
        if (commandLine.hasOption(PRIVATE_KEY_PATH)) {
            return Optional.of(commandLine.getOptionValue(PRIVATE_KEY_PATH));
        }
        return Optional.empty();
    }
}
