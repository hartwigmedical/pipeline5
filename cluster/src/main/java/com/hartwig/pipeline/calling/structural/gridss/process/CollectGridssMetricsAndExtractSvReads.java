package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

public class CollectGridssMetricsAndExtractSvReads implements BashCommand {

    private final String inputBam;
    private String insertSizeMetrics;

    public CollectGridssMetricsAndExtractSvReads(final String inputFile, String insertSizeMetrics) {
        this.inputBam = inputFile;
        this.insertSizeMetrics = insertSizeMetrics;
    }

    @Override
    public String asBash() {
        return GridssCommon.gridssCommand("gridss.CollectGridssMetricsAndExtractSVReads", "4G", gridssArgs()).asBash();
    }

    public String resultantBam() {
        return VmDirectories.outputFile("gridss_sv.bam");
    }

    public String resultantMetrics() {
        return VmDirectories.outputFile("gridss_sv.metrics");
    }

    private String gridssArgs() {
        return new GridssArguments()
                .add("tmp_dir", "/tmp")
                .add("assume_sorted", "true")
                .add("i", inputBam)
                .add("o", VmDirectories.OUTPUT)
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
                .add("include_duplicates", format("true | samtools sort -O bam -T /tmp/samtools.sort.tmp -n -l 0 -@ 2 -o %s", resultantBam()))
        .asBash();
    }
}
