package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.execution.vm.command.BashCommand;
import com.hartwig.pipeline.execution.vm.command.unix.RedirectStdoutCommand;

import org.junit.Test;

public class RedirectStdoutCommandTest {
    @Test
    public void createsValidCommand() {
        String outputFile = "/some/output/file";
        BashCommand cmd = mock(BashCommand.class);
        when(cmd.asBash()).thenReturn("some arbitrary command");
        assertThat(new RedirectStdoutCommand(cmd, outputFile).asBash()).isEqualTo(format("(some arbitrary command 1> %s)", outputFile));
    }
}