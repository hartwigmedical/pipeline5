package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

public class CollectGridssMetricsAndExtractSvReads implements BashCommand {

    private final String inputBam;
    private String insertSizeMetrics;
    private String sampleName;

    public CollectGridssMetricsAndExtractSvReads(final String inputFile, String insertSizeMetrics, String sampleName) {
        this.inputBam = inputFile;
        this.insertSizeMetrics = insertSizeMetrics;
        this.sampleName = sampleName;
    }

    @Override
    public String asBash() {
        return GridssCommon.gridssCommand("gridss.CollectGridssMetricsAndExtractSVReads", "4G", gridssArgs()).asBash();
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

    private String gridssArgs() {
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
                .add("include_duplicates", format("true | %s sort -O bam -T /tmp/samtools.sort.tmp -n -l 0 -@ 2 -o %s",
                        GridssCommon.pathToSamtools(), resultantBam()))
        .asBash();
    }
}
