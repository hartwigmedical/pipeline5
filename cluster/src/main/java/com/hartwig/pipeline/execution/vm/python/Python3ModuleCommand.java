package com.hartwig.pipeline.execution.vm.python;

import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class Python3ModuleCommand implements BashCommand {
    private final String toolName;
    private final String version;
    private final String moduleName;
    private final List<String> arguments;

    public Python3ModuleCommand(final String toolName, final String version, final String moduleName, final List<String> arguments) {
        this.toolName = toolName;
        this.version = version;
        this.moduleName = moduleName;
        this.arguments = arguments;
    }

    @Override
    public String asBash() {
        return String.format("source %s/%s/%s_venv/bin/activate; python -m %s %s; deactivate",
                VmDirectories.TOOLS,
                toolName,
                version,

                moduleName,
                String.join(" ", arguments));
    }
}
