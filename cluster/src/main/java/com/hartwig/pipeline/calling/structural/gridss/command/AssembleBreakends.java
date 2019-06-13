package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AssembleBreakends implements GridssCommand {
    private final String referenceGenome;
    private final String sampleBam;
    private final String tumorBam;
    private final String assemblyBam;

    public AssembleBreakends(final String sampleBam, final String tumorBam, final String referenceGenome) {
        this.referenceGenome = referenceGenome;
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = VmDirectories.outputFile("reference-tumor.assembly.bam");
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
        return 31;
    }

    @Override
    public String arguments() {
         return new GridssArguments()
                        .addTempDir()
                        .add("working_dir", VmDirectories.OUTPUT)
                        .add("reference_sequence", referenceGenome)
                        .add("input", sampleBam)
                        .add("input", tumorBam)
                        .add("output", assemblyBam)
                        .addBlacklist()
                        .addConfigFile()
                        .asBash();
    }
}
