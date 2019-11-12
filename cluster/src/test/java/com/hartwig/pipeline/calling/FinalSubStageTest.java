package com.hartwig.pipeline.calling;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

import org.junit.Test;

public class FinalSubStageTest {

    @Test
    public void appendsFinalConventionToOutputFile() {
        CaptureOutputFile capture = new CaptureOutputFile();
        FinalSubStage victim = FinalSubStage.of(capture);
        victim.apply(SubStageInputOutput.empty("sample"));
        assertThat(capture.outputFile.path()).isEqualTo("/data/output/sample.test.final.vcf");
    }

    private static class CaptureOutputFile extends SubStage {
        private OutputFile outputFile;

        CaptureOutputFile() {
            super("test", OutputFile.VCF);
        }

        @Override
        public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
            outputFile = output;
            return Collections.emptyList();
        }
    }

}