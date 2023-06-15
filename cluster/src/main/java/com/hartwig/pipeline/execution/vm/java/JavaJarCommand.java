package com.hartwig.pipeline.execution.vm.java;

import static java.lang.String.format;

import static com.hartwig.pipeline.tools.ToolInfo.PILOT_VERSION;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.ToolInfo;

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

    public JavaJarCommand(final ToolInfo toolInfo, final List<String> arguments) {
        this.toolName = toolInfo.ToolName;
        this.version = toolInfo.runVersion();
        this.jar = toolInfo.jar();
        this.maxHeapSize = toolInfo.maxHeapStr();
        this.arguments = arguments;
    }

    @Override
    public String asBash() {
        return format("java -Xmx%s -jar %s/%s/%s/%s %s",
                maxHeapSize,
                VmDirectories.TOOLS,
                toolName,
                version,
                jar,
                arguments.stream().collect(Collectors.joining(" ")));
    }
}
