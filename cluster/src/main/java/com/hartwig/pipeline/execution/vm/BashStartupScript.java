package com.hartwig.pipeline.execution.vm;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class BashStartupScript {
    private static final String COMPLETION_FLAG_FILENAME = "JOB_COMPLETE";
    private static final String LOG_FILE = VmDirectories.OUTPUT + "/run.log";
    private final List<String> commands;
    private final String runtimeBucketName;

    private BashStartupScript(final String runtimeBucketName) {
        this.runtimeBucketName = runtimeBucketName;
        this.commands = new ArrayList<>();
    }

    public static BashStartupScript of(final String runtimeBucketName) {
        return new BashStartupScript(runtimeBucketName);
    }

    /**
     * @return the generated script as a single <code>String</code> with UNIX newlines separating input lines
     */
    String asUnixString() {
        String commandSuffix = format(" >>%s 2>&1 || die", LOG_FILE);
        String preamble = format("JOB_COMPLETE_FLAG=\"/tmp/%s\"\n\n", COMPLETION_FLAG_FILENAME) +
                "set -x\n" +
                "function die() {\n" +
                "  exit_code=$?\n" +
                "  echo \"Unknown failure: called command returned $exit_code\"\n" +
                format("  gsutil -m cp %s gs://%s\n", LOG_FILE, runtimeBucketName) +
                "  echo $exit_code > $JOB_COMPLETE_FLAG\n" +
                format("  gsutil -m cp $JOB_COMPLETE_FLAG gs://%s\n", runtimeBucketName) +
                "  exit 1\n" +
                "}\n\n";
        addCompletionCommands();
        return "#!/bin/bash -x\n\n" + preamble + commands.stream()
                .collect(joining(format("%s\n", commandSuffix))) + (commands.isEmpty() ? "" : commandSuffix);
    }

    public BashStartupScript addLine(String lineOne) {
        commands.add(lineOne);
        return this;
    }

    public BashStartupScript addCommand(BashCommand command){
        return addLine(command.asBash());
    }

    String completionFlag() {
        return COMPLETION_FLAG_FILENAME;
    }

    private void addCompletionCommands() {
        commands.add(format("(echo 0 > $JOB_COMPLETE_FLAG && gsutil cp $JOB_COMPLETE_FLAG gs://%s)",
                runtimeBucketName));
    }
}
