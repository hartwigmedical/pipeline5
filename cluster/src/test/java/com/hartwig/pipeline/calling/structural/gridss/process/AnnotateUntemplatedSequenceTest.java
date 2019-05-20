package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AnnotateUntemplatedSequenceTest implements CommonEntities {
    private String className;
    private static final String OUTFILE = String.format("%s/annotated.vcf", OUT_DIR);
    private AnnotateUntemplatedSequence command;
    private String inputVcf;

    @Before
    public void setup() {
        className = "gridss.AnnotateUntemplatedSequence";
        inputVcf = "/intermediate.vcf";
        command = new AnnotateUntemplatedSequence(inputVcf, REFERENCE_GENOME);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassname() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(className, "8G");
    }

    @Test
    public void shouldEndCommandLineWithGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, inputVcf)
                .and(ARG_KEY_OUTPUT, command.resultantVcf())
                .and(ARG_KEY_WORKER_THREADS, "2")
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

    @Test
    public void shouldReturnResultantVcf() {
        assertThat(command.resultantVcf()).isNotNull();
        assertThat(command.resultantVcf()).isEqualTo(OUTFILE);
    }
}
