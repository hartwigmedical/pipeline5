package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class CollectGridssMetricsTest implements CommonEntities  {
    private CollectGridssMetrics command;
    private String inputFile;
    private String outputMetrics;

    @Before
    public void setup() {
        inputFile = "input_file";
        outputMetrics = format("%s/collect_gridss.metrics", OUT_DIR);
        command = new CollectGridssMetrics(inputFile);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo("gridss.analysis.CollectGridssMetrics");

    }

    @Test
    public void shouldUseStandardAmountOfMemory() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldCompleteCommandLineWithGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments("tmp_dir", "/tmp")
                .and("assume_sorted", "true")
                .and("i", inputFile)
                .and("o", outputMetrics)
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
                .andNoMore();
    }

    @Test
    public void shouldReturnMetrics() {
        assertThat(command.metrics()).isNotNull();
        assertThat(command.metrics()).isEqualTo(outputMetrics);
    }
}
