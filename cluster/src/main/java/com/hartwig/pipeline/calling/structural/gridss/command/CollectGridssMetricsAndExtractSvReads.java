package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class CollectGridssMetricsAndExtractSvReads extends GridssCommand {
    private final String inputBam;
    private final String sampleName;
    private final String insertSizeMetrics;
    private final String outputFullPathPrefix;
    private final String workingDirectory;

    public CollectGridssMetricsAndExtractSvReads(final String inputBam, final String sampleName, final String insertSizeMetrics,
            final String outputFullPathPrefix, final String workingDirectory) {
        this.inputBam = inputBam;
        this.sampleName = sampleName;
        this.insertSizeMetrics = insertSizeMetrics;
        this.outputFullPathPrefix = outputFullPathPrefix;
        this.workingDirectory = workingDirectory;
    }

    @Override
    public List<GridssArgument> arguments() {
        return ImmutableList.of(GridssArgument.ASSUME_SORTED,
                new GridssArgument("i", inputBam),
                new GridssArgument("o", outputFullPathPrefix),
                new GridssArgument("threshold_coverage", "50000"),
                GridssArgument.NULL_FILE_EXTENSION,
                new GridssArgument("gridss_program", "null"),
                new GridssArgument("gridss_program", "CollectCigarMetrics"),
                new GridssArgument("gridss_program", "CollectMapqMetrics"),
                new GridssArgument("gridss_program", "CollectTagMetrics"),
                new GridssArgument("gridss_program", "CollectIdsvMetrics"),
                new GridssArgument("gridss_program", "ReportThresholdCoverage"),
                new GridssArgument("program", "null"),
                new GridssArgument("program", "CollectInsertSizeMetrics"),
                new GridssArgument("sv_output", "/dev/stdout"),
                GridssArgument.NO_COMPRESSION,
                new GridssArgument("metrics_output", format("%s/%s.sv_metrics", workingDirectory, sampleName)),
                new GridssArgument("insert_size_metrics", insertSizeMetrics),
                new GridssArgument("unmapped_reads", "false"),
                new GridssArgument("min_clip_length", "5"),
                new GridssArgument("include_duplicates", "true"));
    }

    public String resultantBam() {
        return VmDirectories.outputFile(format("gridss.tmp.querysorted.%s.sv.bam", sampleName));
    }

    @Override
    public String className() {
        return "gridss.CollectGridssMetricsAndExtractSVReads";
    }
}
