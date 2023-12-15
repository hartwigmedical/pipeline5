package com.hartwig.pipeline.execution.vm.unix;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.command.unix.MkDirCommand;

public class MkDirCommandTest {

    @Test
    public void createsBashToMakeADirectory() {
        MkDirCommand victim = new MkDirCommand("test");
        assertThat(victim.asBash()).isEqualTo("mkdir -p test");
    }
}