package com.hartwig.pipeline.execution.vm.command;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class BatchInputDownloadCommand implements BashCommand {
    private final InputDownloadCommand[] inputs;

    public BatchInputDownloadCommand(final InputDownloadCommand... inputs) {
        this.inputs = inputs;
    }

    @Override
    public String asBash() {
        return String.format("gsutil -qm cp %s %s",
                Arrays.stream(inputs).map(InputDownloadCommand::getRemoteSourcePath).collect(joining(" ")),
                VmDirectories.INPUT);
    }
}
