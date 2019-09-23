package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageTest;
import com.hartwig.pipeline.calling.structural.gridss.command.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.File;

import static com.hartwig.pipeline.calling.structural.gridss.GridssTestConstants.*;
import static com.hartwig.pipeline.calling.structural.gridss.stage.BashAssertions.assertBashContains;
import static com.hartwig.pipeline.testsupport.TestConstants.OUT_DIR;
import static java.lang.String.format;
import static org.mockito.Mockito.*;

public class AssembleTest extends SubStageTest {
    private BashStartupScript initialScript;
    private String workingDirectory;
    private ArgumentCaptor<BashCommand> captor;

    @Override
    public SubStage createVictim() {
        return new Assemble(REFERENCE_BAM, TUMOR_BAM, JOINT_NAME, REFERENCE_GENOME, CONFIG_FILE, BLACKLIST);
    }

    @Override
    public String expectedPath() {
        return format("%s/%s.assemble.bam", OUT_DIR, sampleName());
    }

    @Before
    public void setup() {
        workingDirectory = format("%s.gridss.working", ASSEMBLY_BAM);
        OutputFile input = mock(OutputFile.class);
        OutputFile output = mock(OutputFile.class);
        initialScript = mock(BashStartupScript.class);
        BashStartupScript finishedScript = createVictim().bash(input, output, initialScript);
        captor = ArgumentCaptor.forClass(BashCommand.class);
        verify(finishedScript, times(4)).addCommand(captor.capture());
    }

    @Test
    public void shouldAddCommandsInCorrectOrder() {
        InOrder inOrder = Mockito.inOrder(initialScript);
        inOrder.verify(initialScript).addCommand(any(MkDirCommand.class));
        inOrder.verify(initialScript).addCommand(any(AssembleBreakends.class));
        inOrder.verify(initialScript).addCommand(any(CollectGridssMetrics.class));
        inOrder.verify(initialScript).addCommand(any(SoftClipsToSplitReads.ForAssemble.class));
    }

    @Test
    public void shouldAddCorrectAssembleBreakends() {
        assertBashContains(new AssembleBreakends(REFERENCE_BAM, TUMOR_BAM, ASSEMBLY_BAM, REFERENCE_GENOME, CONFIG_FILE, BLACKLIST), captor);
    }

    @Test
    public void shouldAddCorrectMkDirForWorkingDirectory() {
        assertBashContains(new MkDirCommand(workingDirectory), captor);
    }

    @Test
    public void shouldAddCorrectCollectGridssMetrics() {
        String assemblyBasename = new File(ASSEMBLY_BAM).getName();
        assertBashContains(new CollectGridssMetrics(ASSEMBLY_BAM, format("%s/%s", workingDirectory, assemblyBasename)), captor);
    }

    @Test
    public void shouldAddCorrectSoftClipsToSplitReads() {
        String outputBam = format("%s/%s_%s.assemble.bam.sv.bam", workingDirectory, REFERENCE_SAMPLE, TUMOR_SAMPLE);
        assertBashContains(new SoftClipsToSplitReads.ForAssemble(ASSEMBLY_BAM, REFERENCE_GENOME, outputBam), captor);
    }
}