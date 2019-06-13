package com.hartwig.pipeline.calling.germline.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.execution.vm.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

import org.jetbrains.annotations.NotNull;

class SnpSiftCommand extends JavaJarCommand {
    SnpSiftCommand(final String command, final String configPath, final String... args) {
        super("snpEff", Versions.SNPEFF, "SnpSift.jar", GermlineCaller.TOOL_HEAP, arguments(command, configPath, args));
    }

    @NotNull
    private static List<String> arguments(final String command, final String configPath, final String[] args) {
        List<String> additionalArguments = new ArrayList<>();
        additionalArguments.add(command);
        additionalArguments.add("-c");
        additionalArguments.add(configPath);
        additionalArguments.addAll(Arrays.asList(args));
        return additionalArguments;
    }
}