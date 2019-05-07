package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.process.CollectGridssMetricsAndExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.process.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.process.SoftClipsToSplitReads;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class PreprocessTest {
    private String inputBam;
    private String referenceGenome;
    private String collectorBam;
    private String computeSamTagsBam;
    private String clippedBam;
    private String collectorMetrics;

    private ComputeSamTags computeSamTags;
    private SoftClipsToSplitReads.ForPreprocess clips;
    private CollectGridssMetricsAndExtractSvReads collector;
    private CommandFactory factory;
    private Preprocess.PreprocessResult result;

    @Before
    public void setup() {
        inputBam = "/some-path/some-file.bam";
        referenceGenome = "/path/to/ref_genome";

        collectorBam = inputBam + ".collected";
        collectorMetrics = "sv_metrics";

        computeSamTagsBam = collectorBam + ".computed";
        clippedBam = computeSamTagsBam + ".clipped";

        factory = mock(CommandFactory.class);

        collector = mock(CollectGridssMetricsAndExtractSvReads.class);
        when(factory.buildCollectGridssMetricsAndExtractSvReads(any())).thenReturn(collector);
        when(collector.resultantMetrics()).thenReturn(collectorMetrics);
        when(collector.resultantBam()).thenReturn(collectorBam);
        when(collector.asBash()).thenReturn("collector bash");

        computeSamTags = mock(ComputeSamTags.class);
        when(factory.buildComputeSamTags(any(), any())).thenReturn(computeSamTags);
        when(computeSamTags.resultantBam()).thenReturn(computeSamTagsBam);
        when(computeSamTags.asBash()).thenReturn("compute sam tags bash");

        clips = mock(SoftClipsToSplitReads.ForPreprocess.class);
        when(factory.buildSoftClipsToSplitReadsForPreProcess(any(), eq(referenceGenome))).thenReturn(clips);
        when(clips.resultantBam()).thenReturn(clippedBam);
        when(clips.asBash()).thenReturn("soft clips to split reads bash");

        result = new Preprocess(factory).initialise(inputBam, referenceGenome, "");
    }

    @Test
    public void shouldRequestCollectionCommandFromFactoryPassingInputBam() {
        verify(factory).buildCollectGridssMetricsAndExtractSvReads(inputBam);
    }

    @Test
    public void shouldRequestComputeSamTagsFromFactoryPassingOutputFromPreviousCommandAndRefGenome() {
        verify(factory).buildComputeSamTags(collectorBam, referenceGenome);
    }

    @Test
    public void shouldRequestSoftClipsToSplitReadsFromFactoryPassingBamFromPreviousCommand() {
        verify(factory).buildSoftClipsToSplitReadsForPreProcess(computeSamTagsBam, referenceGenome);
    }

    @Test
    public void shouldSetBamInResult() {
        assertThat(result.svBam()).isEqualTo(clippedBam);
    }

    @Test
    public void shouldSetMetricsInResult() {
        assertThat(result.metrics()).isEqualTo(collectorMetrics);
    }

    @Test
    public void shouldSetBashCommandInResultToConcatenationOfBashFromEachCommandInOrder() {
        assertThat(result.commands()).isEqualTo(asList(collector, computeSamTags, clips));
    }
}