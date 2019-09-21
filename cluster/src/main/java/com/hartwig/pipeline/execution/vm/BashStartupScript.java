package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.execution.vm.storage.StorageStrategy;

public class BashStartupScript {
    static final String JOB_SUCCEEDED_FLAG = "JOB_SUCCESS";
    static final String JOB_FAILED_FLAG = "JOB_FAILURE";
    static final String LOG_FILE = "/var/log/run.log";
    private final List<String> commands;
    private final String runtimeBucketName;

    private BashStartupScript(final String runtimeBucketName) {
        this.runtimeBucketName = runtimeBucketName;
        this.commands = new ArrayList<>();
        this.commands.add("echo $(date) Starting run");
        this.commands.add("mkdir -p /data/input");
        this.commands.add("mkdir -p /data/resources");
        this.commands.add("mkdir -p /data/output");
        this.commands.add("mkdir -p /data/tmp");
        this.commands.add("export TMPDIR=/data/tmp");
    }

    public static BashStartupScript of(final String runtimeBucketName) {
        return new BashStartupScript(runtimeBucketName);
    }

    public String asUnixString() {
        return asUnixString(new StorageStrategy() {});
    }

    public String asUnixString(StorageStrategy storageStrategy) {
        String commandSuffix = format(" >>%s 2>&1 || die", LOG_FILE);
        String jobFailedFlag = "/tmp/" + JOB_FAILED_FLAG;

        List<String> preamble = new ArrayList<>(asList(
                "#!/bin/bash -x\n",
                "set -o pipefail\n",
                "function die() {",
                "  exit_code=$?",
                "  echo \"Unknown failure: called command returned $exit_code\"",
                format("  gsutil -m cp %s gs://%s", LOG_FILE, runtimeBucketName),
                format("  echo $exit_code > %s", jobFailedFlag),
                format("  gsutil -m cp %s gs://%s", jobFailedFlag, runtimeBucketName),
                "  exit $exit_code\n" + "}\n"));
        preamble.addAll(storageStrategy.initialise());
        addCompletionCommands();
        return preamble.stream().collect(joining("\n")) + "\n" +
                commands.stream().collect(joining(format("%s\n", commandSuffix))) +
                (commands.isEmpty() ? "" : commandSuffix);
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

    public BashStartupScript addCommands(List<? extends BashCommand> commands) {
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
        return JOB_SUCCEEDED_FLAG;
    }

    String failureFlag() {
        return JOB_FAILED_FLAG;
    }
}
