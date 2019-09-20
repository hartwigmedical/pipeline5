package com.hartwig.pipeline.calling;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.testsupport.CommonTestEntities;

import org.junit.Test;

public class FinalSubStageTest implements CommonTestEntities {

    @Test
    public void appendsFinalConventionToOutputFile() {
        CaptureOutputFile capture = new CaptureOutputFile();
        FinalSubStage victim = FinalSubStage.of(capture);
        victim.apply(SubStageInputOutput.of("sample", OutputFile.empty(), BashStartupScript.of("bucket")));
        assertThat(capture.outputFile.path()).isEqualTo(outFile("sample.test.final.vcf"));
    }

    private static class CaptureOutputFile extends SubStage{
        private OutputFile outputFile;

        CaptureOutputFile() {
            super("test", OutputFile.VCF);
        }

        @Override
        public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
            outputFile = output;
            return bash;
        }
    }

}