package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CpCommandTest {
    @Test
    public void shouldCreateCopyCommand() {
        String source = "/some/file/somewhere";
        String target = "/some/file/somewhere_else";
        assertThat(new CpCommand(source, target).asBash()).isEqualTo(format("cp %s %s", source, target));
    }
}