package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class GrepCommand implements BashCommand {
    private final String grep;

    public GrepCommand(final String grep) {
        this.grep = grep;
    }

    @Override
    public String asBash() {
        return format("grep %s", grep);
    }
}
