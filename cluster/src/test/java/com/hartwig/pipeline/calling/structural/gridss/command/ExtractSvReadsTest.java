package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import org.junit.Before;
import org.junit.Test;

public class ExtractSvReadsTest implements CommonEntities {
    private String metricsOut;
    private String inputFile;
    private String insertSizeMetrics;
    private ExtractSvReads command;

    @Before
    public void setup() {
        inputFile = "inputFile";
        insertSizeMetrics = "insertSizeMetrics";
        String workingDir = format("%s/%s.gridss.working/", VmDirectories.OUTPUT, REFERENCE_SAMPLE);
        metricsOut = format("%s/%s.sv_metrics", workingDir, REFERENCE_SAMPLE);

        command = new ExtractSvReads(inputFile, REFERENCE_SAMPLE, insertSizeMetrics, workingDir);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo("gridss.ExtractSVReads");
    }

    @Test
    public void shouldUseStandardAmountOfMemory() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldConstructGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments(ARGS_TMP_DIR)
                .and("assume_sorted", "true")
                .and(ARG_KEY_INPUT_SHORT, inputFile)
                .and(ARG_KEY_OUTPUT_SHORT, "/dev/stdout")
                .and(ARGS_NO_COMPRESSION)
                .and("metrics_output", metricsOut)
                .and("insert_size_metrics", insertSizeMetrics)
                .and("unmapped_reads", "false")
                .and("min_clip_length", "5")
                .and("include_duplicates", "true")
                .andNoMore();
    }

    @Test
    public void shouldReturnMetrics() {
        assertThat(command.resultantMetrics()).isNotNull();
        assertThat(command.resultantMetrics()).isEqualTo(metricsOut);
    }
}