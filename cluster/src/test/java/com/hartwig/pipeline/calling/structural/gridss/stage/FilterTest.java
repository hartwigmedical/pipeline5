package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.command.BiocondaVariantAnnotationWorkaround;
import com.hartwig.pipeline.calling.structural.gridss.command.RscriptFilter;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class FilterTest {
    private String uncompressedVcf;
    private String outputFilteredVcf;
    private String outputFullVcf;

    private BashStartupScript initialScript;
    private ArgumentCaptor<BashCommand> captor;
    private OutputFile input;
    private OutputFile output;

    @Before
    public void setup() {
        input = mock(OutputFile.class);
        output = mock(OutputFile.class);

        uncompressedVcf = outFile("original.vcf");
        when(input.path()).thenReturn(uncompressedVcf + ".gz");
        outputFilteredVcf = "filtered.vcf";
        outputFullVcf = "full.vcf";

        captor = ArgumentCaptor.forClass(BashCommand.class);

        initialScript = mock(BashStartupScript.class);
        BashStartupScript finishedScript = new Filter(outputFilteredVcf, outputFullVcf).bash(input, output, initialScript);
        verify(finishedScript, times(6)).addCommand(captor.capture());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfInputPathDoesNotEndWithGz() {
        when(input.path()).thenReturn(uncompressedVcf);
        new Filter(outputFilteredVcf, outputFullVcf).bash(input, output, initialScript);
    }

    @Test
    public void shouldRunBiocondaWorkaroundAsFirstStepPassingDeducedUncompressedFilenameAsOutput() {
        BiocondaVariantAnnotationWorkaround command = new BiocondaVariantAnnotationWorkaround(input.path(), uncompressedVcf);
        assertThat(captor.getAllValues().get(0).asBash()).isEqualTo(format("(%s)", command.asBash()));
    }

    @Test
    public void shouldRunRscriptWithCorrectScriptAsSecondStep() {
        String expectedRscript = new RscriptFilter(uncompressedVcf, outputFilteredVcf, outputFullVcf).asBash();
        assertThat(captor.getAllValues().get(1).asBash()).isEqualTo(expectedRscript);
    }

    @Test
    public void shouldMoveInterimFullVcfAndTbiToFinalLocationAfterRscriptRuns() {
        assertThat(captor.getAllValues().get(2).asBash()).isEqualTo(format("mv %s.bgz %s.gz", outputFullVcf, outputFullVcf));
        assertThat(captor.getAllValues().get(3).asBash()).isEqualTo(format("mv %s.bgz.tbi %s.gz.tbi", outputFullVcf, outputFullVcf));
    }

    @Test
    public void shouldMoveInterimFilteredVcfAndTbiToFinalLocation() {
        assertThat(captor.getAllValues().get(4).asBash()).isEqualTo(format("mv %s.bgz %s.gz", outputFilteredVcf, outputFilteredVcf));
        assertThat(captor.getAllValues().get(5).asBash()).isEqualTo(format("mv %s.bgz.tbi %s.gz.tbi", outputFilteredVcf, outputFilteredVcf));
    }
}