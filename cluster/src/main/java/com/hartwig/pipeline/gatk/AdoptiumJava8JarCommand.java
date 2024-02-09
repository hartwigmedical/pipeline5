package com.hartwig.pipeline.gatk;

import com.hartwig.computeengine.execution.vm.command.BashCommand;

import java.util.List;

public class AdoptiumJava8JarCommand implements BashCommand {
    private final String toolName;
    private final String version;
    private final String jar;
    private final String maxHeapSize;
    private final List<String> arguments;

    public AdoptiumJava8JarCommand(String toolName, String version, String jar, String maxHeapSize, List<String> arguments) {
        this.toolName = toolName;
        this.version = version;
        this.jar = jar;
        this.maxHeapSize = maxHeapSize;
        this.arguments = arguments;
    }

    public String asBash() {
        return String.format("/usr/lib/jvm/jdk8u302-b08/jre/bin/java -Xmx%s -jar %s/%s/%s/%s %s",
                this.maxHeapSize,
                "/opt/tools",
                this.toolName,
                this.version,
                this.jar,
                String.join(" ", this.arguments));
    }
}