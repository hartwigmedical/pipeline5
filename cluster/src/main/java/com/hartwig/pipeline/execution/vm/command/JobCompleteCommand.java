package com.hartwig.pipeline.execution.vm.command;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.command.BashCommand;

public class JobCompleteCommand implements BashCommand {

    private final String flag;

    public JobCompleteCommand(final String flag) {
        this.flag = flag;
    }

    @Override
    public String asBash() {
        return String.format("date > %s/%s", VmDirectories.OUTPUT, flag);
    }
}
