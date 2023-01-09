package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class TeeCommand implements BashCommand {
    private final String outputFilePath;

    public TeeCommand(final String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    @Override
    public String asBash() {
        return format("tee %s", outputFilePath);
    }
}
