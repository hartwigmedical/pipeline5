package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class ComputeSamTags implements GridssCommand {
    private final String inProgressBam;
    private final String referenceGenome;
    private final String sampleName;

    public ComputeSamTags(final String inProgressBam, final String referenceGenome, final String sampleName) {
        this.inProgressBam = inProgressBam;
        this.referenceGenome = referenceGenome;
        this.sampleName = sampleName;
    }

    public String resultantBam() {
        return VmDirectories.outputFile(format("gridss.tmp.withtags.%s.sv.bam", sampleName));
    }

    @Override
    public String arguments() {
        return new GridssArguments()
                .add("tmp_dir", GridssCommon.tmpDir())
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
                .add("assume_sorted", "true").asBash();
    }

    @Override
    public String className() {
        return "gridss.ComputeSamTags";
    }
}
