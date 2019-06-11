package com.hartwig.pipeline.execution.vm.unix;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class ExportVariableCommand implements BashCommand {
    private final String variable;
    private final String value;

    public ExportVariableCommand(String variable, String value) {
        this.variable = variable;
        this.value = value;
    }

    @Override
    public String asBash() {
        return String.format("export %s=\"%s\"", variable, value);
    }
}
