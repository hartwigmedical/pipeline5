package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class CollectGridssMetrics implements GridssCommand {
    private final String inputFile;

    public CollectGridssMetrics(final String inputFile) {
        this.inputFile = inputFile;
    }

    @Override
    public String arguments() {
        return new GridssArguments()
                        .add("tmp_dir", "/tmp")
                        .add("assume_sorted", "true")
                        .add("i", inputFile)
                        .add("o", metrics())
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

    public String metrics() {
        return VmDirectories.outputFile("collect_gridss.metrics");
    }

    @Override
    public String className() {
        return "gridss.analysis.CollectGridssMetrics";
    }
}
