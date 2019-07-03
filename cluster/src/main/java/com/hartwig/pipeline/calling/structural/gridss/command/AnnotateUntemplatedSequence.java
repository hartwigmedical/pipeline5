package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AnnotateUntemplatedSequence implements GridssCommand {

    private final String inputVcf;
    private final String referenceGenome;
    private final String outputVcf;

    public AnnotateUntemplatedSequence(final String inputVcf, final String referenceGenome, final String tumorSampleName) {
        this.inputVcf = inputVcf;
        this.referenceGenome = referenceGenome;
        this.outputVcf = VmDirectories.outputFile(tumorSampleName + ".gridss.unfiltered.vcf");
    }

    public String resultantVcf() {
        return outputVcf;
    }

    @Override
    public String className() {
        return "gridss.AnnotateUntemplatedSequence";
    }

    @Override
    public String arguments() {
        return new GridssArguments().add("reference_sequence", referenceGenome)
                .add("input", inputVcf)
                .add("output", resultantVcf())
                .asBash();
    }
}
