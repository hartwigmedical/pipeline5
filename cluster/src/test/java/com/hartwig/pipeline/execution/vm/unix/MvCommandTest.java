package com.hartwig.pipeline.execution.vm.unix;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.command.unix.MvCommand;

import org.junit.Test;

public class MvCommandTest {
    @Test
    public void shouldConstructBash() {
        assertThat(new MvCommand("/path/to/source", "dest").asBash()).isEqualTo("mv /path/to/source dest");
    }
}