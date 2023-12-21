package com.hartwig.pipeline;

import java.util.Objects;

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
        return arguments.runTag()
                .or(() -> arguments.sbpApiRunId().map(Objects::toString))
                .map(tag -> RunIdentifier.from(metadata.runName(), tag))
                .orElse(RunIdentifier.from(metadata.runName()));
    }

}
