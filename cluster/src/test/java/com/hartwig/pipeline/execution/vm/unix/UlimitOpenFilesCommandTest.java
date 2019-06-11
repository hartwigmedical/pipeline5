package com.hartwig.pipeline.execution.vm.unix;

import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class UlimitOpenFilesCommandTest {
    @Test
    public void shouldGenerateCommand() {
        int newValue = 10;
        assertThat(new UlimitOpenFilesCommand(newValue).asBash()).isEqualTo(format("ulimit -n %d", newValue));
    }

}