package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.command.BashCommand;

public class JobComplete implements BashCommand {

    private final String flag;

    public JobComplete(final String flag) {
        this.flag = flag;
    }

    @Override
    public String asBash() {
        return format("date > %s/%s", VmDirectories.OUTPUT, flag);
    }
}
