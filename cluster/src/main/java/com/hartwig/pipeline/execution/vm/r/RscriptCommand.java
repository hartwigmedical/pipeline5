package com.hartwig.pipeline.execution.vm.r;

import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class RscriptCommand implements BashCommand {
    private final String toolName;
    private final String version;
    private final String rFile;
    private final List<String> arguments;

    public RscriptCommand(final String toolName, final String version, final String rFile, final List<String> arguments) {
        this.toolName = toolName;
        this.version = version;
        this.rFile = rFile;
        this.arguments = arguments;
    }

    @Override
    public String asBash() {
        return String.format("Rscript %s/%s/%s/%s %s", VmDirectories.TOOLS, toolName, version, rFile, String.join(" ", arguments));
    }
}
