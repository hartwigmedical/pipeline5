package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetricsAndExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.command.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PreprocessTest implements CommonEntities {
    private String collectMetricsAndExtractReadsBam;
    private String computeSamTagsBam;
    private String collectorMetrics;

    private String collectorBashCommands;
    private String computeSamTagsBashCommands;
    private String clipsBashCommands;

    private ComputeSamTags computeSamTags;
    private SoftClipsToSplitReads.ForPreprocess clips;
    private CollectGridssMetricsAndExtractSvReads collector;
    private CommandFactory factory;
    private GridssToBashCommandConverter converter;
    private Preprocess.PreprocessResult result;

    @Before
    public void setup() {
        collectMetricsAndExtractReadsBam = REFERENCE_BAM + ".collected";
        collectorMetrics = "sv_metrics";
        computeSamTagsBam = collectMetricsAndExtractReadsBam + ".computed";

        factory = mock(CommandFactory.class);
        converter = mock(GridssToBashCommandConverter.class);

        collector = mock(CollectGridssMetricsAndExtractSvReads.class);
        when(factory.buildCollectGridssMetricsAndExtractSvReads(any(), any())).thenReturn(collector);
        when(collector.resultantMetrics()).thenReturn(collectorMetrics);
        when(collector.resultantBam()).thenReturn(collectMetricsAndExtractReadsBam);

        computeSamTags = mock(ComputeSamTags.class);
        when(factory.buildComputeSamTags(any(), any(), any())).thenReturn(computeSamTags);
        when(computeSamTags.resultantBam()).thenReturn(computeSamTagsBam);

        clips = mock(SoftClipsToSplitReads.ForPreprocess.class);
        when(factory.buildSoftClipsToSplitReadsForPreProcess(any(), any(), any())).thenReturn(clips);

        collectorBashCommands = "collector bash commands";
        JavaClassCommand collectorBash = mock(JavaClassCommand.class);
        when(converter.convert(collector)).thenReturn(collectorBash);
        when(collectorBash.asBash()).thenReturn(collectorBashCommands);

        computeSamTagsBashCommands = "compute sam tags bash commands";
        JavaClassCommand computeSamTagsBash = mock(JavaClassCommand.class);
        when(converter.convert(computeSamTags)).thenReturn(computeSamTagsBash);
        when(computeSamTagsBash.asBash()).thenReturn(computeSamTagsBashCommands);

        clipsBashCommands = "clips bash commands";
        JavaClassCommand clipsBash = mock(JavaClassCommand.class);
        when(converter.convert(clips)).thenReturn(clipsBash);
        when(clipsBash.asBash()).thenReturn(clipsBashCommands);

        result = new Preprocess(factory, converter).initialise(REFERENCE_BAM,
                REFERENCE_SAMPLE, REFERENCE_GENOME, OUTPUT_BAM);
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
        String firstSort = format("%s sort -O bam -T /tmp/samtools.sort.tmp -n -l 0 -@ 2 -o %s",
                PATH_TO_SAMTOOLS, collectMetricsAndExtractReadsBam);
        String stageOne = format("(%s | %s)", collectorBashCommands, firstSort);

        String secondSort = format("%s sort -O bam -T /tmp/samtools.sort.tmp -@ 2 -o %s",
                PATH_TO_SAMTOOLS, computeSamTagsBam);
        String stageTwo = format("(%s | %s)", computeSamTagsBashCommands, secondSort);

        List<BashCommand> generatedCommands = result.commands();
        assertThat(generatedCommands).isNotEmpty();
        assertThat(generatedCommands.size()).isEqualTo(3);
        assertThat(generatedCommands.get(0).asBash()).isEqualTo(stageOne);
        assertThat(generatedCommands.get(1).asBash()).isEqualTo(stageTwo);
        assertThat(generatedCommands.get(2).asBash()).isEqualTo(clipsBashCommands);
    }
}