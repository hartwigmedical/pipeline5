package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.io.File;
import java.util.List;

public class CollectGridssMetrics extends GridssCommand {
    private final String inputBam;
    private final String workingDirectory;

    public CollectGridssMetrics(final String inputBam, final String workingDirectory) {
        this.inputBam = inputBam;
        this.workingDirectory = workingDirectory;
    }

    @Override
    public List<GridssArgument> arguments() {
        return asList(GridssArgument.tempDir(),
                new GridssArgument("assume_sorted", "true"),
                new GridssArgument("i", inputBam),
                new GridssArgument("o", outputBaseFilename()),
                new GridssArgument("threshold_coverage", "50000"),
                new GridssArgument("file_extension", "null"),
                new GridssArgument("gridss_program", "null"),
                new GridssArgument("gridss_program", "CollectCigarMetrics"),
                new GridssArgument("gridss_program", "CollectMapqMetrics"),
                new GridssArgument("gridss_program", "CollectTagMetrics"),
                new GridssArgument("gridss_program", "CollectIdsvMetrics"),
                new GridssArgument("gridss_program", "ReportThresholdCoverage"),
                new GridssArgument("program", "null"),
                new GridssArgument("program", "CollectInsertSizeMetrics"));
    }

    public String outputBaseFilename() {
        return format("%s/%s", workingDirectory, new File(inputBam).getName());
    }

    @Override
    public String className() {
        return "gridss.analysis.CollectGridssMetrics";
    }
}
