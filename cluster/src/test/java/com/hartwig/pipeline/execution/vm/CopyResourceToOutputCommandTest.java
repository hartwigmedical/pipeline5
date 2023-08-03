package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.command.CopyResourceToOutputCommand;

import org.junit.Test;

public class CopyResourceToOutputCommandTest {
    @Test
    public void shouldCopyResourceToRootOfOutput() {
        CopyResourceToOutputCommand victim = new CopyResourceToOutputCommand("/path/to/a/local/resource.file");
        assertThat(victim.asBash()).isEqualTo(format("cp /path/to/a/local/resource.file %s", VmDirectories.OUTPUT));
    }
}