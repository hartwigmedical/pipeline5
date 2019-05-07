package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class ComputeSamTags implements BashCommand {
    private String inProgressBam;
    private String referenceGenome;

    public ComputeSamTags(String inProgressBam, String referenceGenome) {
        this.inProgressBam = inProgressBam;
        this.referenceGenome = referenceGenome;
    }

    @Override
    public String asBash() {
        return GridssCommon.gridssCommand("gridss.ComputeSamTags", "4G", arguments()).asBash();
    }

    private String arguments() {
        return new GridssArguments()
                .add("tmp_dir", "/tmp")
                .add("working_dir", VmDirectories.OUTPUT)
                .add("reference_sequence", referenceGenome)
                .add("compression_level", "0")
                .add("i", inProgressBam)
                .add("o", "/dev/stdout")
                .add("recalculate_sa_supplementary", "true")
                .add("soften_hard_clips", "true")
                .add("fix_mate_information", "true")
                .add("fix_duplicate_flag", "true")
                .add("tags", "null")
                .add("tags", "NM")
                .add("tags", "SA")
                .add("tags", "R2")
                .add("tags", "Q2")
                .add("tags", "MC")
                .add("tags", "MQ")
                .add("assume_sorted", String.format("true | samtools sort -O bam -T /tmp/samtools.sort.tmp -@ 2 -o %s", resultantBam()))
        .asBash();
    }

    public String resultantBam() {
        return VmDirectories.outputFile("compute_sam_tags.bam");
    }
}
