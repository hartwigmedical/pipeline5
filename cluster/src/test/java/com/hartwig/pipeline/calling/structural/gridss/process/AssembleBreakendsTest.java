package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.calling.structural.gridss.TestConstants;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;

public class AssembleBreakendsTest {
    private static final String CLASSNAME = "gridss.AssembleBreakends";

    private AssembleBreakends command;
    private String sampleBam;
    private String tumorBam;
    private String blacklist;

    @Before
    public void setup() {
        sampleBam = "/patient123_r";
        tumorBam = "/patient123_t";
        blacklist = format("%s/gridss.blacklist", TestConstants.RESOURCE_DIR);

        command = new AssembleBreakends(sampleBam, tumorBam, TestConstants.REF_GENOME, blacklist);
    }

    @Test
    public void shouldCreateCommandLineStartingWithJavaCommandAndJvmArgumentsAndClassName() {
        GridssCommonArgumentsAssert.assertThat(command).hasJvmArgsAndClassName(CLASSNAME, "31G");
    }

    @Test
    public void shouldCompleteCommandLineWithGridssArguments() {
        GridssCommonArgumentsAssert.assertThat(command).hasGridssArguments("tmp_dir", "/tmp")
                .and("working_dir", TestConstants.OUT_DIR)
                .and("reference_sequence", TestConstants.REF_GENOME)
                .and("input", sampleBam)
                .and("input", tumorBam)
                .and("output", command.resultantBam())
                .and("worker_threads", "2")
                .and("blacklist", blacklist)
                .and("configuration_file", command.resultantConfig())
                .andNoMore()
                .andGridssArgumentsAfterClassnameAreCorrect(CLASSNAME);
    }
}
