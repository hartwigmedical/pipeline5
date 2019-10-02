package com.hartwig.pipeline.calling.structural.gridss.command;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;

import org.junit.Before;
import org.junit.Test;

public class PreProcessSoftClipsToSplitReadsTest implements CommonEntities {
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
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments("working_dir", OUT_DIR)
                .and("reference_sequence", REFERENCE_GENOME)
                .and("i", TUMOR_BAM)
                .and("o", OUTPUT_BAM)
                .andNoMore();
    }
}