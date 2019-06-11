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
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments("tmp_dir", "/tmp")
                .and("working_dir", OUT_DIR)
                .and("reference_sequence", REFERENCE_GENOME)
                .and("i", TUMOR_BAM)
                .and("o", OUTPUT_BAM)
                .and("aligner_command_line", "null")
                .and("aligner_command_line", PATH_TO_BWA)
                .and("aligner_command_line", "mem")
                .and("'aligner_command_line", "-K 40000000'")
                .and("aligner_command_line", "-t")
                .and("'aligner_command_line", "%3$d'")
                .and("'aligner_command_line", "%2$s'")
                .and("'aligner_command_line", "%1$s'")
                .andNoMore();
    }
}