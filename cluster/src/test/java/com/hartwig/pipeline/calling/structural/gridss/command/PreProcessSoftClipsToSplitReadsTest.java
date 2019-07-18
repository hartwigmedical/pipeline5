package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PreProcessSoftClipsToSplitReadsTest implements CommonEntities {
    private SoftClipsToSplitReads.ForPreprocess command;

    @Before
    public void setup() {
        command = new SoftClipsToSplitReads.ForPreprocess(TUMOR_BAM, REFERENCE_GENOME, OUTPUT_BAM);
    }

    @Test
    public void shouldReturnClassName() {
        assertThat(command.className()).isEqualTo("gridss.SoftClipsToSplitReads");
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