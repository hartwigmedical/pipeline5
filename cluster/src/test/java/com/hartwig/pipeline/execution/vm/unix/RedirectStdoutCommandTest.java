package com.hartwig.pipeline.execution.vm.unix;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.command.SamtoolsCommand;

import org.junit.Test;

public class RedirectStdoutCommandTest {
    @Test
    public void shouldCreateValidRedirectionCommand() {
        String grepString = "view file.bam";
        String outputFile = "/some/output/file";
        assertThat(new RedirectStdoutCommand(new SamtoolsCommand(grepString), outputFile).asBash()).isEqualTo(format("(/opt/tools/samtools/1.14/samtools %s 1> %s)",
                grepString,
                outputFile));
    }
}