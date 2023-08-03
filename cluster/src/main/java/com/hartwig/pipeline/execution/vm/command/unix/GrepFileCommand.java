package com.hartwig.pipeline.execution.vm.command.unix;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class GrepFileCommand implements BashCommand {
    private final String inputFile;
    private final String grep;

    public GrepFileCommand(final String inputFile, final String grep) {
        this.inputFile = inputFile;
        this.grep = grep;
    }

    @Override
    public String asBash() {
        return format("grep %s %s", grep, inputFile);
    }
}
