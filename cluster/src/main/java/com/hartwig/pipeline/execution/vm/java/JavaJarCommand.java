package com.hartwig.pipeline.execution.vm.java;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class JavaJarCommand implements BashCommand {

    private final String toolName;
    private final String version;
    private final String jar;
    private final String maxHeapSize;
    private final List<String> arguments;

    public JavaJarCommand(final String toolName, final String version, final String jar, final String maxHeapSize,
            final List<String> arguments) {
        this.toolName = toolName;
        this.version = version;
        this.jar = jar;
        this.maxHeapSize = maxHeapSize;
        this.arguments = arguments;
    }

    @Override
    public String asBash() {
        return String.format("java -Xmx%s -jar %s/%s/%s/%s %s",
                maxHeapSize,
                VmDirectories.TOOLS,
                toolName,
                version,
                jar,
                String.join(" ", arguments));
    }
}
