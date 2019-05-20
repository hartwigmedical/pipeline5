package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;

public class PreProcessSoftClipsToSplitReadsTest implements CommonEntities {
    private SoftClipsToSplitReads.ForPreprocess command;
    private String className;

    @Before
    public void setup() {
        className = "gridss.SoftClipsToSplitReads";
        command = new SoftClipsToSplitReads.ForPreprocess(TUMOR_BAM, REFERENCE_GENOME, OUTPUT_BAM);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassName() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(
                asList("-Dgridss.output_to_temp_file=true"), className, "4G");
    }

    @Test
    public void shouldCreateCommandLineEndingWithGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments("tmp_dir", "/tmp")
                .and("working_dir", OUT_DIR)
                .and("reference_sequence", REFERENCE_GENOME)
                .and("i", TUMOR_BAM)
                .and("o", OUTPUT_BAM)
                .and("worker_threads", "2")
                .and("aligner_command_line", "null")
                .and("aligner_command_line", PATH_TO_BWA)
                .and("aligner_command_line", "mem")
                .and("'aligner_command_line", "-K 40000000'")
                .and("aligner_command_line", "-t")
                .and("'aligner_command_line", "%3$d'")
                .and("'aligner_command_line", "%2$s'")
                .and("'aligner_command_line", "%1$s'")
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(className);
    }
}