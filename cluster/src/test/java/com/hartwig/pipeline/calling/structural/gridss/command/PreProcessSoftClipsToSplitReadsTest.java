package com.hartwig.pipeline.calling.structural.gridss.command;

import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.calling.structural.gridss.GridssTestConstants.*;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static org.assertj.core.api.Assertions.assertThat;

public class PreProcessSoftClipsToSplitReadsTest {
    private SoftClipsToSplitReads.ForPreprocess command;
    private String className;

    @Before
    public void setup() {
        command = new SoftClipsToSplitReads.ForPreprocess(TUMOR_BAM, REFERENCE_GENOME, OUTPUT_BAM);
        className = "gridss.SoftClipsToSplitReads";
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
    public void shouldGenerateGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and("working_dir", OUT_DIR)
                .and("reference_sequence", REFERENCE_GENOME)
                .and("i", TUMOR_BAM)
                .and("o", OUTPUT_BAM)
                .andNoMore();
    }
}