package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.calling.structural.gridss.process.CollectGridssMetricsAndExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.process.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.process.SoftClipsToSplitReads;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class PreprocessTest implements CommonEntities {
    private String collectorBam;
    private String computeSamTagsBam;
    private String collectorMetrics;
    private String insertSizeMetrics;

    private ComputeSamTags computeSamTags;
    private SoftClipsToSplitReads.ForPreprocess clips;
    private CollectGridssMetricsAndExtractSvReads collector;
    private CommandFactory factory;
    private Preprocess.PreprocessResult result;

    @Before
    public void setup() {
        insertSizeMetrics = "/some-other-path/insert.metrics";
        collectorBam = REFERENCE_BAM + ".collected";
        collectorMetrics = "sv_metrics";
        computeSamTagsBam = collectorBam + ".computed";

        factory = mock(CommandFactory.class);

        collector = mock(CollectGridssMetricsAndExtractSvReads.class);
        when(factory.buildCollectGridssMetricsAndExtractSvReads(any(), any(), any())).thenReturn(collector);
        when(collector.resultantMetrics()).thenReturn(collectorMetrics);
        when(collector.resultantBam()).thenReturn(collectorBam);
        when(collector.asBash()).thenReturn("collector bash");

        computeSamTags = mock(ComputeSamTags.class);
        when(factory.buildComputeSamTags(any(), any(), any())).thenReturn(computeSamTags);
        when(computeSamTags.resultantBam()).thenReturn(computeSamTagsBam);
        when(computeSamTags.asBash()).thenReturn("compute sam tags bash");

        clips = mock(SoftClipsToSplitReads.ForPreprocess.class);
        when(factory.buildSoftClipsToSplitReadsForPreProcess(any(), any(), any())).thenReturn(clips);
        when(clips.asBash()).thenReturn("soft clips to split reads bash");

        result = new Preprocess(factory).initialise(REFERENCE_BAM, REFERENCE_SAMPLE, REFERENCE_GENOME, insertSizeMetrics, OUTPUT_BAM);
    }

    @Test
    public void shouldRequestCollectionCommandFromFactoryPassingInputBam() {
        verify(factory).buildCollectGridssMetricsAndExtractSvReads(REFERENCE_BAM, insertSizeMetrics, REFERENCE_SAMPLE);
    }

    @Test
    public void shouldRequestComputeSamTagsFromFactoryPassingOutputFromPreviousCommandAndRefGenome() {
        verify(factory).buildComputeSamTags(collectorBam, REFERENCE_GENOME, REFERENCE_SAMPLE);
    }

    @Test
    public void shouldRequestSoftClipsToSplitReadsFromFactoryPassingBamFromPreviousCommand() {
        verify(factory).buildSoftClipsToSplitReadsForPreProcess(computeSamTagsBam, OUTPUT_BAM, REFERENCE_GENOME);
    }

    @Test
    public void shouldSetBamInResult() {
        assertThat(result.svBam()).isEqualTo(OUTPUT_BAM);
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