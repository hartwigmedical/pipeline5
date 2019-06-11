package com.hartwig.pipeline.execution.vm.unix;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SubShellCommandTest {

    @Test
    public void wrapsDecoratedCommandInSubShell() {
        SubShellCommand victim = new SubShellCommand(() -> "my cool script");
        assertThat(victim.asBash()).isEqualTo("(my cool script)");
    }
}