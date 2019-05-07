package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.TestConstants;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class AssembleSoftClipsToSplitReadsTest {
    final static String CLASSNAME = "gridss.SoftClipsToSplitReads";
    private final static String OUTPUT_BAM = format("%s/assemble-soft_clips-to-split_reads.bam", TestConstants.OUT_DIR);

    private SoftClipsToSplitReads.ForAssemble command;
    private String inputBam;

    @Before
    public void setup() {
        inputBam = "/input.bam";
        command = new SoftClipsToSplitReads.ForAssemble(inputBam, TestConstants.REF_GENOME);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassName() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(
                asList("-Dgridss.async.buffersize=16", "-Dgridss.output_to_temp_file=true"),
                CLASSNAME, "8G");
    }

    @Test
    public void shouldCreateCommandLineEndingWithGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments("tmp_dir", "/tmp")
                .and("working_dir", TestConstants.OUT_DIR)
                .and("reference_sequence", TestConstants.REF_GENOME)
                .and("i", inputBam)
                .and("o", OUTPUT_BAM)
                .and("worker_threads", "2")
                .and("aligner_command_line", "null")
                .and("aligner_command_line", TestConstants.PATH_TO_BWA)
                .and("aligner_command_line", "mem")
                .and("aligner_command_line", "-K 40000000")
                .and("aligner_command_line", "-t")
                .and("'aligner_command_line", "%3$d'")
                .and("'aligner_command_line", "%2$s'")
                .and("'aligner_command_line", "%1$s'")
                .and("realign_entire_read", "true")
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(CLASSNAME);
    }

    @Test
    public void shouldSetOutputBam() {
        assertThat(command.resultantBam()).isNotNull();
        assertThat(command.resultantBam()).isEqualTo(OUTPUT_BAM);
    }
}