package com.hartwig.pipeline.calling;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import org.junit.Test;

import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static org.assertj.core.api.Assertions.assertThat;

public class FinalSubStageTest {

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