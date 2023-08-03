package com.hartwig.pipeline.execution.vm.command.python;

import java.util.List;

import com.hartwig.pipeline.execution.vm.command.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class Python3Command implements BashCommand {
    private final String toolName;
    private final String version;
    private final String pythonFile;
    private final List<String> arguments;

    public Python3Command(final String toolName, final String version, final String pythonFile, final List<String> arguments) {
        this.toolName = toolName;
        this.version = version;
        this.pythonFile = pythonFile;
        this.arguments = arguments;
    }

    @Override
    public String asBash() {
        return String.format("%s/%s/%s_venv/bin/python %s/%s/%s/%s %s",
                VmDirectories.TOOLS,
                toolName,
                version,
                VmDirectories.TOOLS,
                toolName,
                version,
                pythonFile,
                String.join(" ", arguments));
    }
}
