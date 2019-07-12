package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AnnotateUntemplatedSequence extends GridssCommand {

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
    public List<GridssArgument> arguments() {
        return Arrays.asList(new GridssArgument("reference_sequence", referenceGenome),
                new GridssArgument("input", inputVcf),
                new GridssArgument("output", resultantVcf()));
    }
}
