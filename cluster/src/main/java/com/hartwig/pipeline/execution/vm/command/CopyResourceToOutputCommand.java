package com.hartwig.pipeline.execution.vm.command;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class CopyResourceToOutputCommand implements BashCommand {
    private final String localResourcePath;

    public CopyResourceToOutputCommand(final String localResourcePath) {
        this.localResourcePath = localResourcePath;
    }

    @Override
    public String asBash() {
        return String.format("cp %s %s", localResourcePath, VmDirectories.OUTPUT);
    }
}
