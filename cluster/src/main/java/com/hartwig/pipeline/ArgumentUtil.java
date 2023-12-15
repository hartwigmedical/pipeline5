package com.hartwig.pipeline;

import com.hartwig.computeengine.execution.vm.ComputeEngineConfig;
import com.hartwig.computeengine.storage.RunIdentifier;
import com.hartwig.pipeline.input.RunMetadata;

public final class ArgumentUtil {
    private ArgumentUtil() {

    }

    public static ComputeEngineConfig toComputeEngineConfig(Arguments arguments) {
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

    public static RunIdentifier toRunIdentifier(Arguments arguments, RunMetadata metadata) {
        if (arguments.runTag().isPresent()) {
            return RunIdentifier.from(metadata.runName(), arguments.runTag().get());
        } else if (arguments.sbpApiRunId().isPresent()) {
            return RunIdentifier.from(metadata.runName(), arguments.sbpApiRunId().get());
        } else {
            return RunIdentifier.from(metadata.runName());
        }
    }

}
