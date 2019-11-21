package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;

public class MakeStrelka implements BashCommand {

    private final String outputDirectory;

    MakeStrelka(final String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    @Override
    public String asBash() {
        return String.format("make -C %s -j %s", outputDirectory, Bash.allCpus());
    }
}