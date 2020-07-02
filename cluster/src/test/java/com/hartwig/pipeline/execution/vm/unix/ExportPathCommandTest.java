package com.hartwig.pipeline.execution.vm.unix;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;

import org.junit.Test;

public class ExportPathCommandTest {

    @Test
    public void shouldTakePathOfCommand() {
        BashCommand command = new ExportPathCommand(new BwaCommand());
        assertThat(command.asBash()).isEqualTo("export PATH=\"${PATH}:/opt/tools/bwa/0.7.17\"");
    }
}
