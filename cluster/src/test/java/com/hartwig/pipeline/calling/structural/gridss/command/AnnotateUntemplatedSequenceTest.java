package com.hartwig.pipeline.calling.structural.gridss.command;

import org.junit.Before;
import org.junit.Test;

import static com.hartwig.pipeline.calling.structural.gridss.GridssTestConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

public class AnnotateUntemplatedSequenceTest {
    private String className;
    private AnnotateUntemplatedSequence command;
    private String inputVcf;
    private String outputVcf;

    @Before
    public void setup() {
        className = "gridss.AnnotateUntemplatedSequence";
        inputVcf = "/intermediate.vcf";
        outputVcf = "/path/to/output.vcf";
        command = new AnnotateUntemplatedSequence(inputVcf, REFERENCE_GENOME, outputVcf);
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
    public void shouldUseStandardGridssHeapSize() {
        GridssCommonArgumentsAssert.assertThat(command).usesStandardAmountOfMemory();
    }

    @Test
    public void shouldConstructGridssOptions() {
        GridssCommonArgumentsAssert.assertThat(command)
                .hasGridssArguments(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, inputVcf)
                .and(ARG_KEY_OUTPUT, outputVcf)
                .andNoMore();
    }
}
