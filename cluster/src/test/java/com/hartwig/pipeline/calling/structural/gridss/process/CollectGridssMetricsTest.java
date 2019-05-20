package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class CollectGridssMetricsTest implements CommonEntities  {
    private CollectGridssMetrics command;
    private String inputFile;
    private static final String CLASSNAME = "gridss.analysis.CollectGridssMetrics";
    private static final String METRICS_OUT = format("%s/collect_gridss.metrics", OUT_DIR);

    @Before
    public void setup() {
        inputFile = "input_file";
        command = new CollectGridssMetrics(inputFile);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassName() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(CLASSNAME, "256M");
    }

    @Test
    public void shouldCompleteCommandLineWithGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments("tmp_dir", "/tmp")
                .and("assume_sorted", "true")
                .and("i", inputFile)
                .and("o", METRICS_OUT)
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
                .andNoMore().andGridssArgumentsAfterClassnameAreCorrect(CLASSNAME);
    }

    @Test
    public void shouldReturnMetrics() {
        assertThat(command.metrics()).isNotNull();
        assertThat(command.metrics()).isEqualTo(METRICS_OUT);
    }
}
