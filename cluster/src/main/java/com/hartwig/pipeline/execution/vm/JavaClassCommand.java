package com.hartwig.pipeline.execution.vm;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JavaClassCommand implements BashCommand {

    private final String toolName;
    private final String version;
    private final String classPath;
    private final String mainClass;
    private final String maxHeapSize;
    private final List<String> arguments;

    public JavaClassCommand(final String toolName, final String version, final String jar, final String mainClass, final String maxHeapSize,
            final String... arguments) {
        this.toolName = toolName;
        this.version = version;
        this.classPath = jar;
        this.mainClass = mainClass;
        this.maxHeapSize = maxHeapSize;
        this.arguments = Arrays.asList(arguments);
    }

    @Override
    public String asBash() {
        return String.format("java -Xmx%s -cp %s/%s/%s/%s %s %s",
                maxHeapSize,
                VmDirectories.TOOLS,
                toolName,
                version,
                classPath,
                mainClass,
                arguments.stream().collect(Collectors.joining(" ")));
    }
}
