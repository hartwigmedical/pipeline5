package com.hartwig.pipeline.execution.vm;

import static java.util.stream.Collectors.joining;

import java.util.Arrays;

public class BatchInputDownload implements BashCommand {
    private final InputDownload[] inputs;

    public BatchInputDownload(final InputDownload... inputs) {
        this.inputs = inputs;
    }

    @Override
    public String asBash() {
        return String.format("gsutil -qm cp %s %s",
                Arrays.stream(inputs).map(InputDownload::getRemoteSourcePath).collect(joining(" ")),
                VmDirectories.INPUT);
    }
}
