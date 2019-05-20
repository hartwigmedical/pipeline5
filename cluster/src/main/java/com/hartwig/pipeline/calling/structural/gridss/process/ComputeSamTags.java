package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

public class ComputeSamTags implements BashCommand {
    private String inProgressBam;
    private String referenceGenome;
    private String sampleName;

    public ComputeSamTags(String inProgressBam, String referenceGenome, String sampleName) {
        this.inProgressBam = inProgressBam;
        this.referenceGenome = referenceGenome;
        this.sampleName = sampleName;
    }

    @Override
    public String asBash() {
        return GridssCommon.gridssCommand("gridss.ComputeSamTags", "4G", arguments()).asBash();
    }

    private String arguments() {
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
                .add("assume_sorted", format("true | %s sort -O bam -T /tmp/samtools.sort.tmp -@ 2 -o %s",
                        GridssCommon.pathToSamtools(), resultantBam()))
        .asBash();
    }

    public String resultantBam() {
        return VmDirectories.outputFile(format("gridss.tmp.withtags.%s.sv.bam", sampleName));
    }
}
