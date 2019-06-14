package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

public class ExtractSvReads implements GridssCommand {

    private final String inputBam;
    private final String sampleName;
    private final String insertSizeMetrics;

    public ExtractSvReads(final String inputFile, final String sampleName, final String insertSizeMetrics) {
        this.inputBam = inputFile;
        this.sampleName = sampleName;
        this.insertSizeMetrics = insertSizeMetrics;
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
                .add("o", "/dev/stdout")
                .add("compression_level", "0")
                .add("metrics_output", resultantMetrics())
                .add("insert_size_metrics", insertSizeMetrics)
                .add("unmapped_reads", "false")
                .add("min_clip_length", "5")
                .add("include_duplicates", "true").asBash();
    }

    @Override
    public String className() {
        return "gridss.ExtractSVReads";
    }
}
