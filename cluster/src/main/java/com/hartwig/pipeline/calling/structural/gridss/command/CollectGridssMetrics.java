package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import java.io.File;

public class CollectGridssMetrics implements GridssCommand {
    private final String inputBam;
    private final String workingDirectory;

    public CollectGridssMetrics(final String inputBam, final String workingDirectory) {
        this.inputBam = inputBam;
        this.workingDirectory = workingDirectory;
    }

    @Override
    public String arguments() {
        return new GridssArguments()
                        .add("tmp_dir", "/tmp")
                        .add("assume_sorted", "true")
                        .add("i", inputBam)
                        .add("o", outputBaseFilename())
                        .add("threshold_coverage", "50000")
                        .add("file_extension", "null")
                        .add("gridss_program", "null")
                        .add("gridss_program", "CollectCigarMetrics")
                        .add("gridss_program", "CollectMapqMetrics")
                        .add("gridss_program", "CollectTagMetrics")
                        .add("gridss_program", "CollectIdsvMetrics")
                        .add("gridss_program", "ReportThresholdCoverage")
                        .add("program", "null")
                        .add("program", "CollectInsertSizeMetrics")
                        .asBash();
    }

    public String outputBaseFilename() {
        return format("%s/%s", workingDirectory, new File(inputBam).getName());
    }

    @Override
    public String className() {
        return "gridss.analysis.CollectGridssMetrics";
    }
}
