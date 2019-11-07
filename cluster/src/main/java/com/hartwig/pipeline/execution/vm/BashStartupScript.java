package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;

import com.hartwig.pipeline.execution.vm.storage.StorageStrategy;

public class BashStartupScript {
    static final String LOCAL_LOG_DIR = "/var/log";
    private final List<String> commands;
    private final String runtimeBucketName;
    private final RuntimeFiles runtimeFiles;

    private BashStartupScript(final String runtimeBucketName, final RuntimeFiles runtimeFiles) {
        this.runtimeBucketName = runtimeBucketName;
        this.runtimeFiles = runtimeFiles;
        this.commands = new ArrayList<>();
        this.commands.add("echo $(date) Starting run");
        this.commands.add("mkdir -p " + VmDirectories.INPUT);
        this.commands.add("mkdir -p " + VmDirectories.RESOURCES);
        this.commands.add("mkdir -p " + VmDirectories.OUTPUT);
        this.commands.add("mkdir -p " + VmDirectories.TEMP);
        this.commands.add("export TMPDIR=" + VmDirectories.TEMP);
        this.commands.add(format("export _JAVA_OPTIONS='-Djava.io.tmpdir=%s'", VmDirectories.TEMP));
    }

    public static BashStartupScript of(final String runtimeBucketName) {
        return new BashStartupScript(runtimeBucketName, RuntimeFiles.typical());
    }

    public static BashStartupScript of(final String runtimeBucketName, RuntimeFiles flags) {
        return new BashStartupScript(runtimeBucketName, flags);
    }

    String asUnixString() {
        return asUnixString(new StorageStrategy() {});
    }

    String asUnixString(StorageStrategy storageStrategy) {
        String localLogFile = format("%s/%s", LOCAL_LOG_DIR, runtimeFiles.log());
        String commandSuffix = format(" >>%s 2>&1 || die", localLogFile);
        String jobFailedFlag = "/tmp/" + runtimeFiles.failure();

        List<String> preamble = new ArrayList<>(asList(
                "#!/bin/bash -x\n",
                "set -o pipefail\n",
                "function die() {",
                "  exit_code=$?",
                "  echo \"Unknown failure: called command returned $exit_code\"",
                format("  gsutil -m cp %s gs://%s", localLogFile, runtimeBucketName),
                format("  echo $exit_code > %s", jobFailedFlag),
                format("  gsutil -m cp %s gs://%s", jobFailedFlag, runtimeBucketName),
                "  exit $exit_code\n" + "}\n"));
        preamble.addAll(storageStrategy.initialise());
        preamble.add("ulimit -n 102400");
        addCompletionCommands();
        return String.join("\n", preamble) + "\n" +
                commands.stream().collect(joining(format("%s\n", commandSuffix))) +
                (commands.isEmpty() ? "" : commandSuffix);
    }

    BashStartupScript addLine(String lineOne) {
        commands.add(lineOne);
        return this;
    }

    public BashStartupScript addCommand(BashCommand command) {
        return addLine(String.format("echo $(date \"+%%Y-%%m-%%d %%H:%%M:%%S\") \"Running command %s with bash: %s\"",
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
        String successFlag = "/tmp/" + runtimeFiles.success();
        commands.add(format("(echo 0 > %s && gsutil cp %s gs://%s)", successFlag, successFlag, runtimeBucketName));
    }
}
