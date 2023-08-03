package com.hartwig.pipeline.execution.vm.command.unix;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class RedirectStdoutCommand implements BashCommand {
    private final String outputFilePath;
    private final BashCommand cmd;

    public RedirectStdoutCommand(final BashCommand cmd, final String outputFilePath) {
        this.cmd = cmd;
        this.outputFilePath = outputFilePath;
    }

    @Override
    public String asBash() {
        return format("(%s 1> %s)", cmd.asBash(), outputFilePath);
    }
}
