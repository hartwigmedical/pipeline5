package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AssembleBreakends extends GridssCommand {
    private final String referenceGenome;
    private final String referenceBam;
    private final String tumorBam;
    private final String assemblyBam;
    private final String configurationFile;
    private final String blacklist;

    public AssembleBreakends(final String referenceBam, final String tumorBam, final String assemblyBam, final String referenceGenome,
            final String configurationFile, final String blacklist) {
        this.referenceGenome = referenceGenome;
        this.referenceBam = referenceBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = assemblyBam;
        this.configurationFile = configurationFile;
        this.blacklist = blacklist;
    }

    @Override
    public String className() {
        return "gridss.AssembleBreakends";
    }

    @Override
    public int memoryGb() {
        return 80;
    }

    @Override
    public List<GridssArgument> arguments() {
        return Arrays.asList(GridssArgument.tempDir(),
                new GridssArgument("working_dir", VmDirectories.OUTPUT),
                new GridssArgument("reference_sequence", referenceGenome),
                new GridssArgument("input", referenceBam),
                new GridssArgument("input", tumorBam),
                new GridssArgument("output", assemblyBam),
                GridssArgument.blacklist(blacklist),
                GridssArgument.configFile(configurationFile)
        );
    }
}
