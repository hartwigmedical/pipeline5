package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AnnotateUntemplatedSequence implements GridssCommand {

    private String inputVcf;
    private String referenceGenome;

    public AnnotateUntemplatedSequence(String inputVcf, String referenceGenome) {
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
