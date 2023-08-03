package com.hartwig.pipeline.execution.vm.command;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;

import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class BatchInputDownloadCommand implements BashCommand {
    private final InputDownload[] inputs;

    public BatchInputDownloadCommand(final InputDownload... inputs) {
        this.inputs = inputs;
    }

    @Override
    public String asBash() {
        return String.format("gsutil -qm cp %s %s",
                Arrays.stream(inputs).map(InputDownload::getRemoteSourcePath).collect(joining(" ")),
                VmDirectories.INPUT);
    }
}
