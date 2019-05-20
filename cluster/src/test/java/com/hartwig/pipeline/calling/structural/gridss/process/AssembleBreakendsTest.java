package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import org.junit.Before;
import org.junit.Test;

public class AssembleBreakendsTest implements CommonEntities {
    private static final String CLASSNAME = "gridss.AssembleBreakends";
    private AssembleBreakends command;

    @Before
    public void setup() {
        command = new AssembleBreakends(REFERENCE_BAM, TUMOR_BAM, REFERENCE_GENOME);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassName() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(CLASSNAME, "31G");
    }

    @Test
    public void shouldCompleteCommandLineWithGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments(ARGS_TMP_DIR)
                .and(ARG_KEY_WORKING_DIR, OUT_DIR)
                .and(ARGS_REFERENCE_SEQUENCE)
                .and(ARG_KEY_INPUT, REFERENCE_BAM)
                .and(ARG_KEY_INPUT, TUMOR_BAM)
                .and(ARG_KEY_OUTPUT, command.assemblyBam())
                .and(ARG_KEY_WORKER_THREADS, "2")
                .and(ARGS_BLACKLIST)
                .and(ARGS_GRIDSS_CONFIG)
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(CLASSNAME);
    }
}
