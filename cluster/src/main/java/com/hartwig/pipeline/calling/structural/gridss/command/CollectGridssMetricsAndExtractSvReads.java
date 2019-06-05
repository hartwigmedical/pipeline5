package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

public class CollectGridssMetricsAndExtractSvReads implements GridssCommand {

    private final String inputBam;
    private String insertSizeMetrics;
    private String sampleName;

    public CollectGridssMetricsAndExtractSvReads(final String inputFile, String insertSizeMetrics, String sampleName) {
        this.inputBam = inputFile;
        this.insertSizeMetrics = insertSizeMetrics;
        this.sampleName = sampleName;
    }

    public String resultantBam() {
        return VmDirectories.outputFile(format("gridss.tmp.querysorted.%s.sv.bam", sampleName));
    }

    public String resultantMetrics() {
        return format("%s.sv_metrics", outputDirectory());
    }

    private String outputDirectory() {
        return VmDirectories.outputFile(format("%s.gridss.working", sampleName));
    }

    @Override
    public String arguments() {
        return new GridssArguments()
                .add("tmp_dir", GridssCommon.tmpDir())
                .add("assume_sorted", "true")
                .add("i", inputBam)
                .add("o", outputDirectory())
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
                .add("sv_output", "/dev/stdout")
                .add("compression_level", "0")
                .add("metrics_output", resultantMetrics())
                .add("insert_size_metrics", insertSizeMetrics)
                .add("unmapped_reads", "false")
                .add("min_clip_length", "5")
                .add("include_duplicates", "true").asBash();
    }

    @Override
    public String className() {
        return "gridss.CollectGridssMetricsAndExtractSVReads";
    }
}
