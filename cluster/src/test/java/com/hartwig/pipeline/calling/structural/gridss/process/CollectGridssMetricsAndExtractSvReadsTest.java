package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class CollectGridssMetricsAndExtractSvReadsTest implements CommonEntities {
    private String className;
    private String metricsOut;
    private String inputFile;
    private String insertSizeMetrics;
    private CollectGridssMetricsAndExtractSvReads command;

    @Before
    public void setup() {
        className = "gridss.CollectGridssMetricsAndExtractSVReads";
        inputFile = "inputFile";
        insertSizeMetrics = "insertSizeMetrics";

        metricsOut = format("%s/%s.gridss.working.sv_metrics", OUT_DIR, REFERENCE_SAMPLE);


        command = new CollectGridssMetricsAndExtractSvReads(inputFile, insertSizeMetrics, REFERENCE_SAMPLE);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassname() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(className, "4G");
    }

    @Test
    public void shouldEndCommandLineWithGridssOptions() {
        String expectedOutput = format("%s/gridss.tmp.querysorted.%s.sv.bam", OUT_DIR, REFERENCE_SAMPLE);
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments(ARGS_TMP_DIR)
                .and("assume_sorted", "true")
                .and(ARG_KEY_INPUT_SHORT, inputFile)
                .and(ARG_KEY_OUTPUT_SHORT, format("%s/%s.gridss.working", OUT_DIR, REFERENCE_SAMPLE))
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
                .and(ARGS_NO_COMPRESSION)
                .and("metrics_output", metricsOut)
                .and("insert_size_metrics", insertSizeMetrics)
                .and("unmapped_reads", "false")
                .and("min_clip_length", "5")
                .and("include_duplicates",
                        format("true | %s sort -O bam -T /tmp/samtools.sort.tmp -n -l 0 -@ 2 -o %s",
                                PATH_TO_SAMTOOLS, expectedOutput))
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(className);
    }

    @Test
    public void shouldReturnMetrics() {
        assertThat(command.resultantMetrics()).isNotNull();
        assertThat(command.resultantMetrics()).isEqualTo(metricsOut);
    }
}