package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AssembleSoftClipsToSplitReadsTest implements CommonEntities {
    private String className;
    private SoftClipsToSplitReads.ForAssemble command;

    @Before
    public void setup() {
        className = "gridss.SoftClipsToSplitReads";
        command = new SoftClipsToSplitReads.ForAssemble(REFERENCE_BAM, REFERENCE_GENOME, OUTPUT_BAM);
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
    public void shouldConstructGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and(ARGS_WORKING_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT_SHORT, REFERENCE_BAM)
                .and(ARG_KEY_OUTPUT_SHORT, OUTPUT_BAM)
                .and(ARG_KEY_WORKER_THREADS, "2")
                .and("aligner_command_line", "null")
                .and("aligner_command_line", PATH_TO_BWA)
                .and("aligner_command_line", "mem")
                .and("'aligner_command_line", "-K 40000000'")
                .and("aligner_command_line", "-t")
                .and("'aligner_command_line", "%3$d'")
                .and("'aligner_command_line", "%2$s'")
                .and("'aligner_command_line", "%1$s'")
                .and("realign_entire_read", "true")
                .andNoMore();
    }
}