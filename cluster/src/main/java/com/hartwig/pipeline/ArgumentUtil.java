package com.hartwig.pipeline;

import java.util.Objects;

import com.hartwig.computeengine.execution.vm.ComputeEngineConfig;
import com.hartwig.computeengine.execution.vm.ImmutableComputeEngineConfig;
import com.hartwig.computeengine.storage.RunIdentifier;
import com.hartwig.pipeline.input.RunMetadata;

public final class ArgumentUtil {
    private ArgumentUtil() {
    }

    public static ComputeEngineConfig toComputeEngineConfig(Arguments arguments) {
        ImmutableComputeEngineConfig.Builder builder = ComputeEngineConfig.builder()
                .project(arguments.project())
                .region(arguments.region())
                .usePreemptibleVms(arguments.usePreemptibleVms())
                .useLocalSsds(arguments.useLocalSsds())
                .vmSelfDeleteOnShutdown(arguments.vmSelfDeleteOnShutdown())
                .network(arguments.network())
                .subnet(arguments.subnet())
                .tags(arguments.tags())
                .pollInterval(arguments.pollInterval())
                .serviceAccountEmail(arguments.serviceAccountEmail())
                .imageName(arguments.imageName())
                .imageProject(arguments.imageProject());

        if (!arguments.machineFamilies().isEmpty()) {
            builder.machineFamilies(arguments.machineFamilies());
        }
        return builder.build();
    }

    public static RunIdentifier toRunIdentifier(Arguments arguments, RunMetadata metadata) {
        return arguments.runTag()
                .or(() -> arguments.sbpApiRunId().map(Objects::toString))
                .map(tag -> RunIdentifier.from(metadata.stagePrefix() + "-" + metadata.runName(), tag))
                .orElse(RunIdentifier.from(metadata.stagePrefix() + "-" + metadata.runName()));
    }

    public static String prettyPrint(Arguments arguments) {
        String s = arguments.toString();
        StringBuilder stringBuilder = new StringBuilder();
        int indent = 4;
        int indentLevel = 0;

        // add indentation
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '{') {
                indentLevel++;
                stringBuilder.append(" {\n").append(" ".repeat(indentLevel * indent));
            }
            else if (c == '}') {
                indentLevel--;
                stringBuilder.append(" }");
            }
            else if (c == ',') {
                stringBuilder.append(",\n").append(" ".repeat(indentLevel * indent - 1));
            }
            else {
                stringBuilder.append(c);
            }
        }
        return stringBuilder.toString();
    }
}
