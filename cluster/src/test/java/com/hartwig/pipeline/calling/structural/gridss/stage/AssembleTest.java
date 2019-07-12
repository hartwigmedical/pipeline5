package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.List;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.calling.structural.gridss.command.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;

import org.junit.Before;
import org.junit.Test;

public class AssembleTest implements CommonEntities {
    private String assembledBam;
    private String fullOutputPath;

    private CommandFactory factory;

    private CollectGridssMetrics collectMetrics;
    private Assemble.AssembleResult result;

    private JavaClassCommand assembleBreakendsBash;
    private JavaClassCommand collectMetricsBash;
    private JavaClassCommand clipsBash;

    @Before
    public void setup() {
        assembledBam = "assembled.bam";
        factory = mock(CommandFactory.class);
        final GridssToBashCommandConverter converter = mock(GridssToBashCommandConverter.class);

        final AssembleBreakends assembleBreakends = mock(AssembleBreakends.class);
        when(factory.buildAssembleBreakends(any(), any(), any(), any())).thenReturn(assembleBreakends);
        when(assembleBreakends.assemblyBam()).thenReturn(assembledBam);

        collectMetrics = mock(CollectGridssMetrics.class);
        when(factory.buildCollectGridssMetrics(any())).thenReturn(collectMetrics);
        when(collectMetrics.outputBaseFilename()).thenReturn("collect_metrics.metrics");

        final SoftClipsToSplitReads.ForAssemble clips = mock(SoftClipsToSplitReads.ForAssemble.class);
        when(factory.buildSoftClipsToSplitReadsForAssemble(any(), any(), any())).thenReturn(clips);

        fullOutputPath = format("%s/%s.gridss.working/%s.sv.bam", OUT_DIR, assembledBam, assembledBam);

        assembleBreakendsBash = mock(JavaClassCommand.class);
        collectMetricsBash = mock(JavaClassCommand.class);
        clipsBash = mock(JavaClassCommand.class);

        when(converter.convert(assembleBreakends)).thenReturn(assembleBreakendsBash);
        when(converter.convert(collectMetrics)).thenReturn(collectMetricsBash);
        when(converter.convert(clips)).thenReturn(clipsBash);

        result = new Assemble(factory, converter).initialise(REFERENCE_BAM, TUMOR_BAM, REFERENCE_GENOME, JOINT_NAME);
    }

    @Test
    public void shouldRequestAssembleBreakendsCommandFromFactoryPassingInputBamsAndReferenceGenome() {
        verify(factory).buildAssembleBreakends(REFERENCE_BAM, TUMOR_BAM, REFERENCE_GENOME, JOINT_NAME);
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
        assertThat(result.svMetrics()).isEqualTo(collectMetrics.outputBaseFilename());
    }

    @Test
    public void shouldReturnBashCommandsInOrder() {
        assertThat(result).isNotNull();
        List<BashCommand> allCommands = result.commands();
        assertThat(allCommands.get(0).asBash()).isEqualTo(format("mkdir -p %s", new File(fullOutputPath).getParent()));
        assertThat(allCommands.get(1)).isEqualTo(assembleBreakendsBash);
        assertThat(allCommands.get(2)).isEqualTo(collectMetricsBash);
        assertThat(allCommands.get(3)).isEqualTo(clipsBash);
    }

    @Test
    public void shouldPassOnAssembleBreakendsAssemblyBamToDownstream() {
        assertThat(result.assemblyBam()).isEqualTo(assembledBam);
    }
}
