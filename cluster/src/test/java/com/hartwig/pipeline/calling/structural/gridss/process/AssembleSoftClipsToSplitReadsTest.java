package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;

public class AssembleSoftClipsToSplitReadsTest implements CommonEntities {
    private String className;
    private SoftClipsToSplitReads.ForAssemble command;

    @Before
    public void setup() {
        className = "gridss.SoftClipsToSplitReads";
        command = new SoftClipsToSplitReads.ForAssemble(REFERENCE_BAM, REFERENCE_GENOME, OUTPUT_BAM);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassName() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(
                asList("-Dgridss.async.buffersize=16", "-Dgridss.output_to_temp_file=true"),
                className, "8G");
    }

    @Test
    public void shouldCreateCommandLineEndingWithGridssArguments() {
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
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(className);
    }
}