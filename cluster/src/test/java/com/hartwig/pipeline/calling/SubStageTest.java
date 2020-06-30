package com.hartwig.pipeline.calling;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

import org.junit.Before;
import org.junit.Test;

public abstract class SubStageTest {

    protected SubStageInputOutput output;

    @Before
    public void setUp() {
        output = createVictim().apply(SubStageInputOutput.of(sampleName(), input(),
                Lists.newArrayList()));
    }

    protected OutputFile input() {
        return OutputFile.of(sampleName(), "strelka", "vcf");
    }

    protected String bash() {
        return output.bash().stream().map(BashCommand::asBash).collect(Collectors.joining());
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
