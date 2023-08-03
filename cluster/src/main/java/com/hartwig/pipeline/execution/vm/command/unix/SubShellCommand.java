package com.hartwig.pipeline.execution.vm.command.unix;

import com.hartwig.pipeline.execution.vm.command.BashCommand;

public class SubShellCommand implements BashCommand {

    private final BashCommand decorated;

    public SubShellCommand(final BashCommand decorated) {
        this.decorated = decorated;
    }

    @Override
    public String asBash() {
        return String.format("(%s)", decorated.asBash());
    }
}
