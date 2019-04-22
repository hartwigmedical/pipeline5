package com.hartwig.pipeline.execution.vm;

import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.execution.vm.BashStartupScript.of;
import static com.hartwig.pipeline.execution.vm.DataFixture.randomStr;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class BashStartupScriptTest {
    private String outputDirectory;
    private String logFile;
    private BashStartupScript scriptBuilder;

    @Before
    public void setup() {
        outputDirectory = randomStr();
        logFile = format("%s/run.log", VmDirectories.OUTPUT);
        scriptBuilder = of(null);
    }

    //@Test
    public void shouldWriteLinesSeparatedByUnixNewlinesWithBlankLineAfterShebang() {
        String lineOne = randomStr();
        String lineTwo = randomStr();

        String output = scriptBuilder.addLine(lineOne).addLine(lineTwo).asUnixString();
        assertThat(output).startsWith(format("%s\n\n", bash()));
    }

    @Test
    public void shouldWriteRedirectionToLogAndFailsafeAfterEveryCommand() {
        String script = scriptBuilder.addLine("hi").addLine("bye").asUnixString();
        boolean found = false;
        for (String line : script.split("\n")) {
            if (line.startsWith("hi") || line.startsWith("bye")) {
                assertThat(line).endsWith(format(" >>%s 2>&1 || die", logFile));
                found = true;
            }
        }
        assertThat(found).isTrue();
    }

    @Test
    public void shouldReturnCompletionFlagFilename() {
        assertThat(scriptBuilder.completionFlag()).isEqualTo("JOB_COMPLETE");
    }

    private String bash() {
        return "#!/bin/bash -ex";
    }
}