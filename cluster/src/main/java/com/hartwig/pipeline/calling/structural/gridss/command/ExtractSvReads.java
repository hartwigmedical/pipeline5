package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class ExtractSvReads extends GridssCommand {

    private final String inputBam;
    private final String sampleName;
    private final String insertSizeMetrics;
    private final String workingDirectory;

    public ExtractSvReads(final String inputFile, final String sampleName, final String insertSizeMetrics, final String workingDirectory) {
        this.inputBam = inputFile;
        this.sampleName = sampleName;
        this.insertSizeMetrics = insertSizeMetrics;
        this.workingDirectory = workingDirectory;
    }

    public String resultantBam() {
        return VmDirectories.outputFile(format("gridss.tmp.querysorted.%s.sv.bam", sampleName));
    }

    public String resultantMetrics() {
        return format("%s/%s.sv_metrics", workingDirectory, sampleName);
    }

    @Override
    public List<GridssArgument> arguments() {
        return Arrays.asList(GridssArgument.tempDir(),
                new GridssArgument("assume_sorted", "true"),
                new GridssArgument("i", inputBam),
                new GridssArgument("o", "/dev/stdout"),
                new GridssArgument("compression_level", "0"),
                new GridssArgument("metrics_output", resultantMetrics()),
                new GridssArgument("insert_size_metrics", insertSizeMetrics),
                new GridssArgument("unmapped_reads", "false"),
                new GridssArgument("min_clip_length", "5"),
                new GridssArgument("include_duplicates", "true"));
    }

    @Override
    public String className() {
        return "gridss.ExtractSVReads";
    }
}
