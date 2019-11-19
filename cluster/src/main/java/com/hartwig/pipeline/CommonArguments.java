package com.hartwig.pipeline;

import java.util.Optional;

public interface CommonArguments {

    String PROJECT = "project";
    String REGION = "region";
    String LOCAL_SSDS = "local_ssds";
    String PREEMPTIBLE_VMS = "preemptible_vms";
    String STORAGE_KEY_PATH = "storage_key_path";
    String SERVICE_ACCOUNT_EMAIL = "service_account_email";
    String CLOUD_SDK = "cloud_sdk";
    String PRIVATE_KEY_PATH = "private_key_path";

    String project();

    String privateKeyPath();

    String cloudSdkPath();

    String region();

    boolean usePreemptibleVms();

    boolean useLocalSsds();

    Optional<String> privateNetwork();

    String serviceAccountEmail();

    Optional<String> cmek();
}
