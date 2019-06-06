package com.hartwig.pipeline.calling;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

import org.junit.Before;
import org.junit.Test;

public abstract class SubStageTest {

    protected SubStageInputOutput output;

    @Before
    public void setUp() throws Exception {
        output = createVictim().apply(SubStageInputOutput.of(sampleName(),
                OutputFile.of(sampleName(), "strelka", "vcf"),
                BashStartupScript.of("runtime_bucket")));
    }

    protected String sampleName() {
        return "tumor";
    }

    public abstract SubStage createVictim();

    public abstract String expectedPath();

    @Test
    public void returnsPathToCorrectOutputFile() {
        assertThat(output.outputFile().path()).isEqualTo(expectedPath());
    }
}
