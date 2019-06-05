package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
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
                .add("worker_threads", "2")
                .add("aligner_command_line", "null")
                .add("aligner_command_line", GridssCommon.pathToBwa())
                .add("aligner_command_line", "mem")
                .add("'aligner_command_line", String.format("-K %d'", GridssCommon.GRIDSS_BWA_BASES_PER_BATCH))
                .add("aligner_command_line", "-t")
                .add("'aligner_command_line", "%3$d'")
                .add("'aligner_command_line", "%2$s'")
                .add("'aligner_command_line", "%1$s'")
                .asBash();
    }
}
