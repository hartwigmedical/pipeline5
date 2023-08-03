package com.hartwig.pipeline.execution.vm.command.unix;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class MkDirCommand implements BashCommand {

    private final String directoryName;

    public MkDirCommand(final String directoryName) {
        this.directoryName = directoryName;
    }

    @Override
    public String asBash() {
        return String.format("mkdir -p %s", directoryName);
    }
}
