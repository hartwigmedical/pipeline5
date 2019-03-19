package com.hartwig.pipeline.cluster.vm;

import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.cluster.vm.BashStartupScript.bashBuilder;
import static com.hartwig.pipeline.cluster.vm.DataFixture.randomStr;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class BashStartupScriptTest {
    private String outputDirectory;
    private String logFile;
    private BashStartupScript scriptBuilder;

    @Before
    public void setup() {
        outputDirectory = randomStr();
        logFile = randomStr();
        scriptBuilder = bashBuilder().outputToDir(outputDirectory).logToFile(logFile);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfProvidedWithNullLine() {
        bashBuilder().addLine(null);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionIfNoLinesAddedBeforeBuild() {
        bashBuilder().asUnixString();
    }

    @Test
    public void shouldWriteLinesSeparatedByUnixNewlinesWithBlankLineAfterShebang() {
        String lineOne = randomStr();
        String lineTwo = randomStr();

        String output = bashBuilder().addLine(lineOne)
                .addLine(lineTwo)
                .asUnixString();
        assertThat(output).isEqualTo(format("%s\n\n%s\n%s", bash(), lineOne, lineTwo));
    }

    @Test
    public void shouldWriteLineToCreateOutputDirectoryButNotRedirectTheOutputToTheLog() {
        assertThat(scriptBuilder.asUnixString()).isEqualToIgnoringWhitespace(format("%s mkdir -p %s",
                bash(), outputDirectory));
    }

    @Test
    public void shouldWriteLineToCreateOutputDirectoryFirstEvenIfItIsNotCalledFirst() {
        String otherCommand = randomStr();
        String output = bashBuilder().addLine(otherCommand).outputToDir(outputDirectory).asUnixString();

        assertThat(output).isEqualToIgnoringWhitespace(format("%s mkdir -p %s %s", bash(), outputDirectory, otherCommand));
    }

    @Test
    public void shouldWriteRedirectionToLogAfterEveryCommand() {
        String script = scriptBuilder.addLine("hi").addLine("bye").asUnixString();
        boolean found = false;
        for (String line: script.split("\n")) {
            if (line.startsWith("hi") || line.startsWith("bye")) {
                assertThat(line).endsWith(format(" >>%s 2>&1", logFile));
                found = true;
            }
        }
        assertThat(found).isTrue();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfNullLogFileIsProvided() {
        bashBuilder().logToFile(null);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfLogFileNameIsJustWhitespace() {
        for (String logFile: asList("", "  ", "\n")) {
            try {
                bashBuilder().logToFile(logFile);
                fail("Expected an exception by now!");
            } catch (IllegalArgumentException iae) {
                // This is what we want
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionIfNullOutputDirectoryIsProvided() {
        bashBuilder().outputToDir(null);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionIfOutputDirectoryIsJustWhitespace() {
        for (String outputDirectory: asList("", "  ", "\n")) {
            try {
                bashBuilder().outputToDir(outputDirectory);
                fail("Expected an exception by now!");
            } catch (IllegalArgumentException iae) {
                // This is what we want
            }
        }
    }

    private String bash() {
        return "#!/bin/bash -ex";
    }
}