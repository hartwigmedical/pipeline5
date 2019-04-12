package com.hartwig.pipeline.cluster.vm;

import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class BashStartupScript {
    private List<String> commands;
    private Optional<String> outputDirectory;
    private Optional<String> logFile;

    private BashStartupScript() {
        this.commands = new ArrayList<>();
        this.outputDirectory = Optional.empty();
        this.logFile = Optional.empty();
    }

    public static BashStartupScript bashBuilder() {
        return new BashStartupScript();
    }

    /**
     * @return the generated script as a single <code>String</code> with UNIx newlines separating input lines
     */
    public String asUnixString() {
        if (commands.isEmpty() && !outputDirectory.isPresent()) {
            throw new IllegalStateException("No commands added yet!");
        }
        String loggingSuffix = logFile.isPresent() ? format(" >>%s 2>&1", logFile.get()) : "";
        return new StringBuilder("#!/bin/bash -ex\n\n")
                .append(outputDirectory.isPresent() ? format("mkdir -p %s\n", outputDirectory.get()) : "")
                .append(commands.stream().collect(joining(format("%s\n", loggingSuffix))))
                .append(commands.isEmpty() ? "" : loggingSuffix)
                .toString();
    }

    public BashStartupScript addLine(String lineOne) {
        Validate.notNull(lineOne, "Cannot add a null line");
        commands.add(lineOne);
        return this;
    }

    public BashStartupScript outputToDir(String outputDirectory) {
        Validate.notNull(outputDirectory, "Output directory must not be null");
        Validate.notEmpty(outputDirectory.trim(), "Output directory must be non-empty");
        this.outputDirectory = Optional.of(outputDirectory);
        return this;
    }

    public BashStartupScript logToFile(String logFile) {
        Validate.notNull(logFile, "Log file must not be null");
        Validate.notEmpty(logFile.trim(), "Log file must be non-empty");
        this.logFile = Optional.of(logFile);
        return this;
    }

    /**
     * @return The final filename component of the file that will be written to indicate the job is complete
     */
    public String completionFlag() {
        return "JOB_COMPLETE";
    }
}
