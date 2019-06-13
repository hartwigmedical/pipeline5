package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AnnotateUntemplatedSequence implements GridssCommand {

    private final String inputVcf;
    private final String referenceGenome;

    public AnnotateUntemplatedSequence(final String inputVcf, final String referenceGenome) {
        this.inputVcf = inputVcf;
        this.referenceGenome = referenceGenome;
    }

    public String resultantVcf() {
        return VmDirectories.outputFile("annotated.vcf");
    }

    @Override
    public String className() {
        return "gridss.AnnotateUntemplatedSequence";
    }

    @Override
    public String arguments() {
        return new GridssArguments()
                .add("reference_sequence", referenceGenome)
                .add("input", inputVcf)
                .add("output", resultantVcf())
                .asBash();
    }
}
