package com.hartwig.pipeline.execution.vm;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class BashStartupScript {
    private static final String JOB_SUCCEEDED_FLAG = "JOB_SUCCESS";
    private static final String JOB_FAILED_FLAG = "JOB_FAILURE";
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
        String jobFailedFlag = "/tmp/" + JOB_FAILED_FLAG;
        String preamble = "#!/bin/bash -x\n\n" +
                "function die() {\n" +
                "  exit_code=$?\n" +
                "  echo \"Unknown failure: called command returned $exit_code\"\n" +
                format("  gsutil -m cp %s gs://%s\n", LOG_FILE, runtimeBucketName) +
                format("  echo $exit_code > %s\n", jobFailedFlag) +
                format("  gsutil -m cp %s gs://%s\n", jobFailedFlag, runtimeBucketName) +
                "  exit $exit_code\n" +
                "}\n\n";
        addCompletionCommands();
        return preamble + commands.stream()
                .collect(joining(format("%s\n", commandSuffix))) + (commands.isEmpty() ? "" : commandSuffix);
    }

    public BashStartupScript addLine(String lineOne) {
        commands.add(lineOne);
        return this;
    }

    public BashStartupScript addCommand(BashCommand command){
        return addLine(command.asBash());
    }

    private void addCompletionCommands() {
        String successFlag = "/tmp/" + JOB_SUCCEEDED_FLAG;
        commands.add(format("(echo 0 > %s && gsutil cp %s gs://%s)", successFlag, successFlag, runtimeBucketName));
    }

    String successFlag() {
        return JOB_SUCCEEDED_FLAG;
    }

    String failureFlag() {
        return JOB_FAILED_FLAG;
    }
}
