package com.hartwig.pipeline.calling.structural.gridss.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.GridssTestEntities;

import org.junit.Before;
import org.junit.Test;

public class CollectGridssMetricsTest implements GridssTestEntities {
    private CollectGridssMetrics command;
    private String inputBamBasename;
    private String inputBamFullPath;
    private String outputMetricsFilepathPrefix;
    private String className;

    @Before
    public void setup() {
        inputBamBasename = "input-file.bam";
        inputBamFullPath = "/full/path/to/" + inputBamBasename;
        outputMetricsFilepathPrefix = "/path/to/metrics/base.name";
        className = "gridss.analysis.CollectGridssMetrics";
        command = new CollectGridssMetrics(inputBamFullPath, outputMetricsFilepathPrefix);

    }

    @Test
    public void shouldGenerateCorrectJavaArguments() {
        GridssCommonArgumentsAssert.assertThat(command).generatesJavaInvocationUpToAndIncludingClassname(className);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo(className);
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
                .and("o", outputMetricsFilepathPrefix)
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
}
