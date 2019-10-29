package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.util.Arrays.asList;

import java.util.List;

public class CollectGridssMetrics extends GridssCommand {
    private final String inputBam;
    private final String outputFullPathPrefix;

    public CollectGridssMetrics(final String inputBam, final String outputFullPathPrefix) {
        this.inputBam = inputBam;
        this.outputFullPathPrefix = outputFullPathPrefix;
    }

    @Override
    public List<GridssArgument> arguments() {
        return asList(GridssArgument.ASSUME_SORTED,
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
                new GridssArgument("program", "CollectInsertSizeMetrics"));
    }

    @Override
    public String className() {
        return "gridss.analysis.CollectGridssMetrics";
    }
}
