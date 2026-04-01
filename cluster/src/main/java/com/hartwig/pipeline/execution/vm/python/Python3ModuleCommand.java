package com.hartwig.pipeline.execution.vm.python;

import java.util.List;

import com.hartwig.computeengine.execution.vm.command.BashCommand;

public class Python3ModuleCommand implements BashCommand {
    private final String moduleName;
    private final List<String> arguments;

    public Python3ModuleCommand(final String moduleName, final List<String> arguments) {
        this.moduleName = moduleName;
        this.arguments = arguments;
    }

    @Override
    public String asBash() {
        // @formatter:off
        return String.format("python3 -m %s %s",
                moduleName,
                String.join(" ", arguments));
        // @formatter:on
    }
}