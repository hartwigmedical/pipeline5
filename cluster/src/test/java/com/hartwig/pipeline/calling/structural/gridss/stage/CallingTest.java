package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.structural.gridss.command.IdentifyVariants;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static com.hartwig.pipeline.calling.structural.gridss.GridssTestConstants.*;
import static com.hartwig.pipeline.calling.structural.gridss.stage.BashAssertions.assertBashContains;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static com.hartwig.pipeline.testsupport.TestConstants.outFile;
import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class CallingTest extends SubStageTest {
    private String assemblyBam;
    private String outputVcf;
    private ArgumentCaptor<BashCommand> captor;

    @Before
    public void setup() {
        assemblyBam = outFile("assembly.bam");
        outputVcf = outFile("output.vcf");
        OutputFile input = mock(OutputFile.class);
        OutputFile output = mock(OutputFile.class);
        BashStartupScript initialScript = mock(BashStartupScript.class);

        when(input.path()).thenReturn(assemblyBam);
        when(output.path()).thenReturn(outputVcf);
        when(initialScript.addCommand(any(BashCommand.class))).thenReturn(initialScript);

        BashStartupScript finishedScript = createVictim().bash(input, output, initialScript);
        captor = ArgumentCaptor.forClass(BashCommand.class);
        verify(finishedScript, times(1)).addCommand(captor.capture());
    }

    @Override
    public SubStage createVictim() {
        return new Calling(REFERENCE_BAM, TUMOR_BAM, REFERENCE_GENOME, CONFIG_FILE, BLACKLIST);
    }

    @Override
    public String expectedPath() {
        return format("%s/%s.calling.vcf", OUT_DIR, JOINT_NAME);
    }

    @Override
    protected String sampleName() {
        return JOINT_NAME;
    }

    @Test
    public void shouldAddVariantCallingCommand() {
        IdentifyVariants variants = new IdentifyVariants(REFERENCE_BAM, TUMOR_BAM, assemblyBam, outputVcf,
                REFERENCE_GENOME, CONFIG_FILE, BLACKLIST);
        assertBashContains(variants, captor);
    }
}
