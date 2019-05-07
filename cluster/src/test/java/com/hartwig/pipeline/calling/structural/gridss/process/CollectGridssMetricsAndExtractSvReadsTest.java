package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.TestConstants;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class CollectGridssMetricsAndExtractSvReadsTest {
    private static final String CLASSNAME = "gridss.CollectGridssMetricsAndExtractSVReads";
    private static final String METRICS_OUT = format("%s/gridss_sv.metrics", TestConstants.OUT_DIR);
    private static final String BAM_OUT = format("%s/gridss_sv.bam", TestConstants.OUT_DIR);

    private String inputFile;
    private String insertSizeMetrics;
    private CollectGridssMetricsAndExtractSvReads command;

    @Before
    public void setup() {
        inputFile = "inputFile";
        insertSizeMetrics = "insertSizeMetrics";

        command = new CollectGridssMetricsAndExtractSvReads(inputFile, insertSizeMetrics);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassname() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(CLASSNAME, "4G");
    }

    @Test
    public void shouldEndCommandLineWithGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments("tmp_dir", "/tmp")
                .and("assume_sorted", "true")
                .and("i", inputFile)
                .and("o", TestConstants.OUT_DIR)
                .and("threshold_coverage", "50000")
                .and("file_extension", "null")
                .and("gridss_program", "null")
                .and("gridss_program", "CollectCigarMetrics")
                .and("gridss_program", "CollectMapqMetrics")
                .and("gridss_program", "CollectTagMetrics")
                .and("gridss_program", "CollectIdsvMetrics")
                .and("gridss_program", "ReportThresholdCoverage")
                .and("program", "null")
                .and("program", "CollectInsertSizeMetrics")
                .and("sv_output", "/dev/stdout")
                .and("compression_level", "0")
                .and("metrics_output", METRICS_OUT)
                .and("insert_size_metrics", insertSizeMetrics)
                .and("unmapped_reads", "false")
                .and("min_clip_length", "5")
                .and("include_duplicates", format("true | samtools sort -O bam -T /tmp/samtools.sort.tmp -n -l 0 -@ 2 -o %s", BAM_OUT))
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(CLASSNAME);
    }

    @Test
    public void shouldReturnMetrics() {
        assertThat(command.resultantMetrics()).isNotNull();
        assertThat(command.resultantMetrics()).isEqualTo(METRICS_OUT);
    }
}