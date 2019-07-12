package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class MvCommand implements BashCommand {
    private final String source;
    private final String destination;

    public MvCommand(final String source, final String destination) {

        this.source = source;
        this.destination = destination;
    }

    @Override
    public String asBash() {
        return format("mv %s %s", source, destination);
    }
}
