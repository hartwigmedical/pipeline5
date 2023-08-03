package com.hartwig.pipeline.execution.vm.command.unix;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.command.BashCommand;

public class CpCommand implements BashCommand {
    private final String source;
    private final String destination;

    public CpCommand(final String source, final String destination) {
        this.source = source;
        this.destination = destination;
    }

    @Override
    public String asBash() {
        return format("cp %s %s", source, destination);
    }
}
