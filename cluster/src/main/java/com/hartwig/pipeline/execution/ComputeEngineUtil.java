package com.hartwig.pipeline.execution;

import com.hartwig.computeengine.execution.vm.ComputeEngineConfig;
import com.hartwig.pipeline.CommonArguments;

public final class ComputeEngineUtil {
    private ComputeEngineUtil() {
    }

    public static ComputeEngineConfig configFromArguments(CommonArguments arguments) {
        return ComputeEngineConfig.builder()
                .project(arguments.project())
                .region(arguments.region())
                .usePreemptibleVms(arguments.usePreemptibleVms())
                .useLocalSsds(arguments.useLocalSsds())
                .network(arguments.network())
                .subnet(arguments.subnet())
                .tags(arguments.tags())
                .pollInterval(arguments.pollInterval())
                .serviceAccountEmail(arguments.serviceAccountEmail())
                .imageName(arguments.imageName())
                .imageProject(arguments.imageProject())
                .build();
    }
}
