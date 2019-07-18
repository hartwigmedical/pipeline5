package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;

import org.junit.Before;
import org.junit.Test;

public class CollectGridssMetricsTest implements CommonEntities {
    private CollectGridssMetrics command;
    private String inputBamBasename;
    private String inputBamFullPath;
    private String outputMetricsFilepathPrefix;
    private String fullOutputMetricsFilepathPrefix;

    @Before
    public void setup() {
        inputBamBasename = "input-file.bam";
        inputBamFullPath = "/full/path/to/" + inputBamBasename;
        outputMetricsFilepathPrefix = format("%s/input-file_metrics", OUT_DIR);
        fullOutputMetricsFilepathPrefix = format("%s/%s", outputMetricsFilepathPrefix, inputBamBasename);
        command = new CollectGridssMetrics(inputBamFullPath, outputMetricsFilepathPrefix);

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
                .hasGridssArguments(ARGS_TMP_DIR)
                .and("assume_sorted", "true")
                .and("i", inputBamFullPath)
                .and("o", fullOutputMetricsFilepathPrefix)
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
        assertThat(command.outputBaseFilename()).isNotEmpty();
        assertThat(command.outputBaseFilename()).isEqualTo(fullOutputMetricsFilepathPrefix);
    }
}
