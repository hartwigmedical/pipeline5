package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AssembleBreakends implements GridssCommand {
    private final String referenceGenome;
    private final String referenceBam;
    private final String tumorBam;
    private final String assemblyBam;

    public AssembleBreakends(final String referenceBam, final String tumorBam, final String referenceGenome, final String jointName) {
        this.referenceGenome = referenceGenome;
        this.referenceBam = referenceBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = VmDirectories.outputFile(jointName + ".assembly.bam");
    }

    public String assemblyBam() {
        return assemblyBam;
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
    public String arguments() {
        return new GridssArguments().addTempDir()
                .add("working_dir", VmDirectories.OUTPUT)
                .add("reference_sequence", referenceGenome)
                .add("input", referenceBam)
                .add("input", tumorBam)
                .add("output", assemblyBam)
                .addBlacklist()
                .addConfigFile()
                .asBash();
    }
}
