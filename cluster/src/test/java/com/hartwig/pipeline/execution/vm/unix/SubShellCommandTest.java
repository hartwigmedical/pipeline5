package com.hartwig.pipeline.execution.vm.unix;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.command.unix.SubShellCommand;

public class SubShellCommandTest {

    @Test
    public void wrapsDecoratedCommandInSubShell() {
        SubShellCommand victim = new SubShellCommand(() -> "my cool script");
        assertThat(victim.asBash()).isEqualTo("(my cool script)");
    }
}