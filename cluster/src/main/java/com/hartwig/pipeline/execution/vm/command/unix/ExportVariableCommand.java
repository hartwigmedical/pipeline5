package com.hartwig.pipeline.execution.vm.command.unix;

import com.hartwig.pipeline.execution.vm.command.BashCommand;

public class ExportVariableCommand implements BashCommand {
    private final String variable;
    private final String value;

    public ExportVariableCommand(final String variable, final String value) {
        this.variable = variable;
        this.value = value;
    }

    @Override
    public String asBash() {
        return String.format("export %s=\"%s\"", variable, value);
    }
}
