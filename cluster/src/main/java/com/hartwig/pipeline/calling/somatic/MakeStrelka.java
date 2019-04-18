package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class MakeStrelka implements BashCommand {

    private final String outputDirectory;
    private final int threads;

    MakeStrelka(final String outputDirectory, final int threads) {
        this.outputDirectory = outputDirectory;
        this.threads = threads;
    }

    @Override
    public String asBash() {
        return String.format("make -C %s -j %s", outputDirectory, threads);
    }
}
