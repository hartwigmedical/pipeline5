package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class CatCommand implements BashCommand {
    private final String inputFilePath;

    public CatCommand(final String inputFilePath) {
        this.inputFilePath = inputFilePath;
    }

    @Override
    public String asBash() {
        return format("cat %s", inputFilePath);
    }
}
