package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static com.hartwig.pipeline.execution.vm.BashStartupScript.of;
import static com.hartwig.pipeline.execution.vm.DataFixture.randomStr;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class BashStartupScriptTest {
    private String outputDirectory;
    private String logFile;
    private BashStartupScript scriptBuilder;

    @Before
    public void setup() {
        outputDirectory = randomStr();
        logFile = randomStr();
        scriptBuilder = of(outputDirectory, logFile);
    }

    @Test
    public void shouldWriteLinesSeparatedByUnixNewlinesWithBlankLineAfterShebang() {
        String lineOne = randomStr();
        String lineTwo = randomStr();

        String output = scriptBuilder.addLine(lineOne).addLine(lineTwo).asUnixString();
        assertThat(output).startsWith(format("%s\n\n", bash()));
    }

    @Test
    public void shouldWriteLineToCreateOutputDirectoryButNotRedirectTheOutputToTheLog() {
        assertThat(scriptBuilder.asUnixString()).isEqualToIgnoringWhitespace(format("%s mkdir -p %s", bash(), outputDirectory));
    }

    @Test
    public void shouldWriteRedirectionToLogAfterEveryCommand() {
        String script = scriptBuilder.addLine("hi").addLine("bye").asUnixString();
        boolean found = false;
        for (String line : script.split("\n")) {
            if (line.startsWith("hi") || line.startsWith("bye")) {
                assertThat(line).endsWith(format(" >>%s 2>&1", logFile));
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