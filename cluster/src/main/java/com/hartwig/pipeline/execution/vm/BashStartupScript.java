package com.hartwig.pipeline.execution.vm;

import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class BashStartupScript {
    private final List<String> commands;
    private final String outputDirectory;
    private final String logFile;

    private BashStartupScript(final String outputDirectory, final String logFile) {
        this.outputDirectory = outputDirectory;
        this.logFile = logFile;
        this.commands = new ArrayList<>();
    }

    public static BashStartupScript of(final String outputDirectory, final String logFile) {
        return new BashStartupScript(outputDirectory, logFile);
    }

    /**
     * @return the generated script as a single <code>String</code> with UNIx newlines separating input lines
     */
    String asUnixString() {
        String loggingSuffix = format(" >>%s 2>&1", logFile);
        return "#!/bin/bash -ex\n\n" + format("mkdir -p %s\n", outputDirectory) + commands.stream()
                .collect(joining(format("%s\n", loggingSuffix))) + (commands.isEmpty() ? "" : loggingSuffix);
    }

    public BashStartupScript addLine(String lineOne) {
        Validate.notNull(lineOne, "Cannot add a null line");
        commands.add(lineOne);
        return this;
    }
    /**
     * @return The final filename component of the file that will be written to indicate the job is complete
     */
    public String completionFlag() {
        return "JOB_COMPLETE";
    }
}
