package com.hartwig.pipeline.execution.vm.command.java;

import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class Java8JarCommand implements BashCommand {

    private final String toolName;
    private final String version;
    private final String jar;
    private final String maxHeapSize;
    private final List<String> arguments;

    public Java8JarCommand(final String toolName, final String version, final String jar, final String maxHeapSize,
            final List<String> arguments) {
        this.toolName = toolName;
        this.version = version;
        this.jar = jar;
        this.maxHeapSize = maxHeapSize;
        this.arguments = arguments;
    }

    @Override
    public String asBash() {
        return String.format("/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java -Xmx%s -jar %s/%s/%s/%s %s",
                maxHeapSize,
                VmDirectories.TOOLS,
                toolName,
                version,
                jar, String.join(" ", arguments));
    }
}
