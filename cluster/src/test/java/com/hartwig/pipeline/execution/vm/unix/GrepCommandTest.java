package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GrepCommandTest {
    @Test
    public void shouldCreateGrepCommand() {
        String grep = "-v '^@PG'";
        assertThat(new GrepCommand(grep).asBash()).isEqualTo(format("grep %s", grep));
    }
}