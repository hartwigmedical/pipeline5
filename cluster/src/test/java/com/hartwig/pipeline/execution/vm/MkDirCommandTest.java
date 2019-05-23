package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class MkDirCommandTest {

    @Test
    public void createsBashToMakeADirectory() {
        MkDirCommand victim = new MkDirCommand("test");
        assertThat(victim.asBash()).isEqualTo("mkdir test");
    }
}