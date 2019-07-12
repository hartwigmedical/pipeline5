package com.hartwig.pipeline.execution.vm.unix;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class GunzipAndKeepArchiveCommandTest {
    @Test
    public void shouldGenerateBash() {
        String archive = "/some/file.gz";
        assertThat(new GunzipAndKeepArchiveCommand(archive).asBash()).isEqualTo("gunzip -kd " + archive);
    }
}