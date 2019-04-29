package com.hartwig.pipeline.calling.somatic;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.BashStartupScript;

import org.junit.Before;
import org.junit.Test;

public abstract class SubStageTest {

    protected SubStageInputOutput output;

    @Before
    public void setUp() throws Exception {
        output = createVictim().apply(SubStageInputOutput.of("tumor",
                OutputFile.of("tumor", "strelka", "vcf"),
                BashStartupScript.of("runtime_bucket")));
    }

    abstract SubStage createVictim();

    abstract String expectedPath();

    @Test
    public void returnsPathToCorrectOutputFile() {
        assertThat(output.outputFile().path()).isEqualTo(expectedPath());
    }
}
