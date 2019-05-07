package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.process.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.process.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.process.SoftClipsToSplitReads;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class AssembleTest {
    private String sampleBam;
    private String tumorBam;
    private String referenceGenome;
    private String blacklist;

    private String assembledBam = "assembled.bam";

    private CommandFactory factory;
    private AssembleBreakends assembleBreakends;
    private CollectGridssMetrics collectMetrics;
    private SoftClipsToSplitReads.ForAssemble clips;
    private Assemble.AssembleResult result;

    @Before
    public void setup() {
        sampleBam = "sample.bam";
        tumorBam = "tumor.bam";
        referenceGenome = "reference_genome";
        blacklist = "blacklist";

        factory = mock(CommandFactory.class);

        assembleBreakends = mock(AssembleBreakends.class);
        when(factory.buildAssembleBreakends(any(), any(), any(), any())).thenReturn(assembleBreakends);
        when(assembleBreakends.resultantBam()).thenReturn(assembledBam);
        when(assembleBreakends.asBash()).thenReturn("assemble breakends bash");

        collectMetrics = mock(CollectGridssMetrics.class);
        when(factory.buildCollectGridssMetrics(any())).thenReturn(collectMetrics);
        when(collectMetrics.asBash()).thenReturn("collect metrics bash");
        when(collectMetrics.metrics()).thenReturn("collect_metrics.metrics");

        clips = mock(SoftClipsToSplitReads.ForAssemble.class);
        when(factory.buildSoftClipsToSplitReadsForAssemble(any(), any())).thenReturn(clips);
        when(clips.asBash()).thenReturn("soft clips to split reads bash");
        when(clips.resultantBam()).thenReturn(assembledBam + ".structural");

        result = new Assemble(factory).initialise(sampleBam, tumorBam, referenceGenome, blacklist);
    }

    @Test
    public void shouldRequestAssembleBreakendsCommandFromFactoryPassingInputBamsAndReferenceGenome() {
        verify(factory).buildAssembleBreakends(sampleBam, tumorBam, referenceGenome, blacklist);
    }

    @Test
    public void shouldRequestCollectGridssMetricsFromFactoryPassingAssembledBam() {
        verify(factory).buildCollectGridssMetrics(assembledBam);
    }

    @Test
    public void shouldRequestSoftClipsToSplitReadsPassingAssembledBamAndReferenceGenome() {
        verify(factory).buildSoftClipsToSplitReadsForAssemble(assembledBam, referenceGenome);
    }

    @Test
    public void shouldSetStructuralBamToResultOfSoftClipsToSplitReads() {
        assertThat(result.svBam()).isEqualTo(clips.resultantBam());
    }

    @Test
    public void shouldSetMetricsToResultOfCollectMetrics() {
        assertThat(result.svMetrics()).isEqualTo(collectMetrics.metrics());
    }

    @Test
    public void shouldReturnBashCommandsInOrder() {
        assertThat(result).isNotNull();
        assertThat(result.commands()).isEqualTo(asList(assembleBreakends, collectMetrics, clips));
    }

}
