package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;

public class BashStartupScript {
    static final String JOB_SUCCEEDED_FLAG_ENDING = "_SUCCESS";
    static final String JOB_FAILED_FLAG_ENDING = "_FAILURE";
    private static final String LOG_FILE = VmDirectories.OUTPUT + "/run.log";
    private final List<String> commands;
    private final String runtimeBucketName;
    private final String flagFilePrefix;

    private BashStartupScript(final String runtimeBucketName, final String flagFilePrefix) {
        this.runtimeBucketName = runtimeBucketName;
        this.flagFilePrefix = flagFilePrefix;
        this.commands = new ArrayList<>();
        this.commands.add("echo $(date) Starting run");
    }

    public static BashStartupScript of(final String runtimeBucketName) {
        return new BashStartupScript(runtimeBucketName, "JOB");
    }

    public static BashStartupScript of(final String runtimeBucketName, String flagFilePrefix) {
        return new BashStartupScript(runtimeBucketName, flagFilePrefix);
    }

    /**
     * @return the generated script as a single <code>String</code> with UNIX newlines separating input lines
     */
    public String asUnixString() {
        String commandSuffix = format(" >>%s 2>&1 || die", LOG_FILE);
        String jobFailedFlag = "/tmp/" + failureFlag();
        String preamble = "#!/bin/bash -x\n\n" + "set -o pipefail\n\n" +
                "function die() {\n" + "  exit_code=$?\n" +
                "  echo \"Unknown failure: called command returned $exit_code\"\n" +
                format("  gsutil -m cp %s gs://%s\n", LOG_FILE, runtimeBucketName) +
                format("  echo $exit_code > %s\n", jobFailedFlag) +
                format("  gsutil -m cp %s gs://%s\n", jobFailedFlag, runtimeBucketName) +
                "  exit $exit_code\n" + "}\n\n";
        addCompletionCommands();
        return preamble + commands.stream().collect(joining(format("%s\n", commandSuffix))) + (commands.isEmpty() ? "" : commandSuffix);
    }

    BashStartupScript addLine(String lineOne) {
        commands.add(lineOne);
        return this;
    }

    public BashStartupScript addCommand(BashCommand command) {
        return addLine(String.format("echo \"Running command %s with bash: %s\"",
                command.getClass().getSimpleName(),
                escapeQuotes(command.asBash()))).addLine(command.asBash());
    }

    private String escapeQuotes(final String s) {
        return s.replace("\"", "\\\"");
    }

    public BashStartupScript addCommands(List<BashCommand> commands) {
        for (BashCommand command : commands) {
            addCommand(command);
        }
        return this;
    }

    private void addCompletionCommands() {
        String successFlag = "/tmp/" + successFlag();
        commands.add(format("(echo 0 > %s && gsutil cp %s gs://%s)", successFlag, successFlag, runtimeBucketName));
    }

    String successFlag() {
        return flagFilePrefix + JOB_SUCCEEDED_FLAG_ENDING;
    }

    String failureFlag() {
        return flagFilePrefix + JOB_FAILED_FLAG_ENDING;
    }
}
