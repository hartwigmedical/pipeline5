package com.hartwig.pipeline.execution.vm.java;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.ToolInfo;

public class JavaClassCommand implements BashCommand {

    private final String toolName;
    private final String version;
    private final String classPath;
    private final String mainClass;
    private final String maxHeapSize;
    private final List<String> arguments;
    private final List<String> jvmArguments;

    public JavaClassCommand(final String toolName, final String version, final String jar, final String mainClass, final String maxHeapSize,
                            final List<String> jvmArguments, final List<String> arguments) {
        this.toolName = toolName;
        this.version = version;
        this.classPath = jar;
        this.mainClass = mainClass;
        this.maxHeapSize = maxHeapSize;
        this.jvmArguments = new ArrayList<>(jvmArguments);
        this.arguments = arguments;
    }

    public JavaClassCommand(final ToolInfo toolInfo, final String mainClass, final List<String> arguments) {
        this(toolInfo.ToolName, toolInfo.runVersion(), toolInfo.jar(), mainClass, toolInfo.maxHeapStr(), Collections.emptyList(), arguments);
    }

    public JavaClassCommand(final String toolName, final String version, final String jar, final String mainClass, final String maxHeapSize,
            final List<String> jvmArguments, final String... arguments) {
        this(toolName, version, jar, mainClass, maxHeapSize, jvmArguments, Arrays.asList(arguments));
    }

    @Override
    public String asBash() {
        String jvmArgsJoined = jvmArguments.stream().collect(Collectors.joining(" ")).trim();
        List<String> tokens = new ArrayList<>();
        tokens.add(format("java -Xmx%s", maxHeapSize));
        if (!jvmArgsJoined.isEmpty()) {
            tokens.add(jvmArgsJoined);
        }
        tokens.add(String.format("-cp %s/%s/%s/%s", VmDirectories.TOOLS, toolName, version, classPath));
        tokens.add(mainClass);
        tokens.add(arguments.stream().collect(Collectors.joining(" ")).trim());
        return tokens.stream().collect(Collectors.joining(" "));
    }
}
