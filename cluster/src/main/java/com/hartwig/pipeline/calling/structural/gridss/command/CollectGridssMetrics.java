package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

public class CollectGridssMetrics implements GridssCommand {
    private final String inputBam;

    public CollectGridssMetrics(final String inputBam) {
        this.inputBam = inputBam;
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

    // This GRIDSS command uses the "-o" argument to mean the fully-qualified basename for the output files. It will
    // generate filenames for each of the requested metrics based upon that starting point.
    public String outputBaseFilename() {
        return VmDirectories.outputFile(format("%s_metrics", GridssCommon.basenameNoExtensions(inputBam)));
    }

    @Override
    public String className() {
        return "gridss.analysis.CollectGridssMetrics";
    }
}
