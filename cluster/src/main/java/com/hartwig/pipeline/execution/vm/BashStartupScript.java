package com.hartwig.pipeline.execution.vm;

import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class BashStartupScript {
    private List<String> commands;
    private String outputDirectory;
    private String logFile;

    private BashStartupScript() {
        this.commands = new ArrayList<>();
    }

    public static BashStartupScript bashBuilder() {
        return new BashStartupScript();
    }

    /**
     * @return the generated script as a single <code>String</code> with UNIx newlines separating input lines
     */
    String asUnixString() {
        if (commands.isEmpty()) {
            throw new IllegalStateException("No commands added yet!");
        }
        String loggingSuffix = format(" >>%s 2>&1", logFile);
        return "#!/bin/bash -ex\n\n" + format("mkdir -p %s\n", outputDirectory) + commands.stream()
                .collect(joining(format("%s\n", loggingSuffix))) + (commands.isEmpty() ? "" : loggingSuffix);
    }

    public BashStartupScript addLine(String lineOne) {
        commands.add(lineOne);
        return this;
    }

    public BashStartupScript outputToDir(String outputDirectory) {
        this.outputDirectory = outputDirectory;
        return this;
    }

    public BashStartupScript logToFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

    /**
     * @return The final filename component of the file that will be written to indicate the job is complete
     */
    public String completionFlag() {
        return "JOB_COMPLETE";
    }
}
