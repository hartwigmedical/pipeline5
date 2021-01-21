package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CopyResourceToOutputTest {
    @Test
    public void shouldCopyResourceToRootOfOutput() {
        CopyResourceToOutput victim = new CopyResourceToOutput("/path/to/a/local/resource.file");
        assertThat(victim.asBash()).isEqualTo(format("cp /path/to/a/local/resource.file %s", VmDirectories.OUTPUT));
    }
}