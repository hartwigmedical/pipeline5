package com.hartwig.pipeline.execution.vm.unix;

import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.command.unix.ExportVariableCommand;

public class ExportVariableCommandTest {
    @Test
    public void shouldExportVariableWithDoubleQuotedValue() {
        String variable = "SOME_VAR";
        String value = "the value of the variable";
        ExportVariableCommand command = new ExportVariableCommand(variable, value);
        assertThat(command.asBash()).isEqualTo(format("export %s=\"%s\"", variable, value));
    }
}