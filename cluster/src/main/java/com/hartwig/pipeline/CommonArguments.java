package com.hartwig.pipeline;

import java.util.Optional;

public interface CommonArguments {
    String project();

    Optional<String> privateKeyPath();

    String cloudSdkPath();

    String region();

    boolean usePreemptibleVms();

    boolean useLocalSsds();

    Optional<String> privateNetwork();

    String serviceAccountEmail();

    Optional<String> cmek();

    boolean shallow();
}
