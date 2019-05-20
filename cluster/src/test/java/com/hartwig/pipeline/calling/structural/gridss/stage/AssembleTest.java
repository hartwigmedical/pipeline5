package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.calling.structural.gridss.process.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.process.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.process.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class AssembleTest implements CommonEntities {
    private String assembledBam;
    private String fullOutputPath;

    private CommandFactory factory;
    private AssembleBreakends assembleBreakends;
    private CollectGridssMetrics collectMetrics;
    private SoftClipsToSplitReads.ForAssemble clips;
    private Assemble.AssembleResult result;

    @Before
    public void setup() {
        assembledBam = "assembled.bam";
        factory = mock(CommandFactory.class);

        assembleBreakends = mock(AssembleBreakends.class);
        when(factory.buildAssembleBreakends(any(), any(), any())).thenReturn(assembleBreakends);
        when(assembleBreakends.assemblyBam()).thenReturn(assembledBam);
        when(assembleBreakends.asBash()).thenReturn("assemble breakends bash");

        collectMetrics = mock(CollectGridssMetrics.class);
        when(factory.buildCollectGridssMetrics(any())).thenReturn(collectMetrics);
        when(collectMetrics.asBash()).thenReturn("collect metrics bash");
        when(collectMetrics.metrics()).thenReturn("collect_metrics.metrics");

        clips = mock(SoftClipsToSplitReads.ForAssemble.class);
        when(factory.buildSoftClipsToSplitReadsForAssemble(any(), any(), any())).thenReturn(clips);
        when(clips.asBash()).thenReturn("soft clips to split reads bash");

        fullOutputPath = format("%s/%s.gridss.working/%s.sv.bam", OUT_DIR, assembledBam, assembledBam);
        result = new Assemble(factory).initialise(REFERENCE_BAM, TUMOR_BAM, REFERENCE_GENOME);
    }

    @Test
    public void shouldRequestAssembleBreakendsCommandFromFactoryPassingInputBamsAndReferenceGenome() {
        verify(factory).buildAssembleBreakends(REFERENCE_BAM, TUMOR_BAM, REFERENCE_GENOME);
    }

    @Test
    public void shouldRequestCollectGridssMetricsFromFactoryPassingAssembledBam() {
        verify(factory).buildCollectGridssMetrics(assembledBam);
    }

    @Test
    public void shouldRequestSoftClipsToSplitReadsPassingAssembledBamAndReferenceGenomeAndAssemblyInProperPath() {
        verify(factory).buildSoftClipsToSplitReadsForAssemble(assembledBam, REFERENCE_GENOME, fullOutputPath);
    }

    @Test
    public void shouldSetMetricsToResultOfCollectMetrics() {
        assertThat(result.svMetrics()).isEqualTo(collectMetrics.metrics());
    }

    @Test
    public void shouldReturnBashCommandsInOrder() {
        assertThat(result).isNotNull();
        List<BashCommand> allCommands = result.commands();
        assertThat(allCommands.get(0).asBash()).isEqualTo(format("mkdir -p %s", new File(fullOutputPath).getParent()));
        assertThat(allCommands.get(1)).isEqualTo(assembleBreakends);
        assertThat(allCommands.get(2)).isEqualTo(collectMetrics);
        assertThat(allCommands.get(3)).isEqualTo(clips);
    }
}
