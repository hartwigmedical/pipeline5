package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.TestConstants;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AnnotateUntemplatedSequenceTest {
    private static final String CLASSNAME = "gridss.AnnotateUntemplatedSequence";
    private static final String OUTFILE = String.format("%s/annotated.vcf", TestConstants.OUT_DIR);
    private AnnotateUntemplatedSequence command;
    private String inputVcf;

    @Before
    public void setup() {
        inputVcf = "/intermediate.vcf";
        command = new AnnotateUntemplatedSequence(inputVcf, TestConstants.REF_GENOME);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassname() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(CLASSNAME, "8G");
    }

    @Test
    public void shouldEndCommandLineWithGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments("reference_sequence", TestConstants.REF_GENOME)
                .and("input", inputVcf)
                .and("output", command.resultantVcf())
                .and("worker_threads", "2")
                .and("aligner_command_line", "null")
                .and("aligner_command_line", TestConstants.PATH_TO_BWA)
                .and("aligner_command_line", "mem")
                .and("aligner_command_line", "-K 40000000")
                .and("aligner_command_line", "-t")
                .and("'aligner_command_line", "%3$d'")
                .and("'aligner_command_line", "%2$s'")
                .and("'aligner_command_line", "%1$s'")
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(CLASSNAME);
    }

    @Test
    public void shouldReturnResultantVcf() {
        assertThat(command.resultantVcf()).isNotNull();
        assertThat(command.resultantVcf()).isEqualTo(OUTFILE);
    }
}
