package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AnnotateUntemplatedSequence implements BashCommand {

    private String inputVcf;
    private String referenceGenome;

    public AnnotateUntemplatedSequence(String inputVcf, String referenceGenome) {
        this.inputVcf = inputVcf;
        this.referenceGenome = referenceGenome;
    }

    @Override
    public String asBash() {
        return GridssCommon.gridssCommand("gridss.AnnotateUntemplatedSequence", "8G", new GridssArguments()
                .add("reference_sequence", referenceGenome)
                .add("input", inputVcf)
                .add("output", resultantVcf())
                .add("worker_threads", "2")
                .add("aligner_command_line", "null")
                .add("aligner_command_line", GridssCommon.pathToBwa())
                .add("aligner_command_line", "mem")
                .add("aligner_command_line", "-K " + GridssCommon.GRIDSS_BWA_BASES_PER_BATCH)
                .add("aligner_command_line", "-t")
                .add("'aligner_command_line", "%3$d'")
                .add("'aligner_command_line", "%2$s'")
                .add("'aligner_command_line", "%1$s'")
                .asBash()
        ).asBash();
    }

    public String resultantVcf() {
        return VmDirectories.outputFile("annotated.vcf");
    }
}
