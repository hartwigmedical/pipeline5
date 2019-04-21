package com.hartwig.pipeline.calling.somatic;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class VersionedToolCommand implements BashCommand {

    private final String toolName;
    private final String toolBinaryName;
    private final String version;
    private final List<String> arguments;

    VersionedToolCommand(final String toolName, final String toolBinaryName, final String version, final String... arguments) {
        this.toolName = toolName;
        this.toolBinaryName = toolBinaryName;
        this.version = version;
        this.arguments = Arrays.asList(arguments);
    }

    @Override
    public String asBash() {
        return String.format("/data/tools/%s/%s/%s %s", toolName, version, toolBinaryName, arguments.stream().collect(joining(" ")));
    }
}
