package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class ComputeSamTags extends GridssCommand {
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
    public List<GridssArgument> arguments() {
        return Arrays.asList(new GridssArgument("working_dir", VmDirectories.OUTPUT),
                new GridssArgument("reference_sequence", referenceGenome),
                new GridssArgument("compression_level", "0"),
                new GridssArgument("i", inProgressBam),
                new GridssArgument("o", "/dev/stdout"),
                new GridssArgument("recalculate_sa_supplementary", "true"),
                new GridssArgument("soften_hard_clips", "true"),
                new GridssArgument("fix_mate_information", "true"),
                new GridssArgument("fix_duplicate_flag", "true"),
                new GridssArgument("tags", "null"),
                new GridssArgument("tags", "NM"),
                new GridssArgument("tags", "SA"),
                new GridssArgument("tags", "R2"),
                new GridssArgument("tags", "Q2"),
                new GridssArgument("tags", "MC"),
                new GridssArgument("tags", "MQ"),
                new GridssArgument("assume_sorted", "true"));
    }
    
    @Override
    public String className() {
        return "gridss.ComputeSamTags";
    }
}
