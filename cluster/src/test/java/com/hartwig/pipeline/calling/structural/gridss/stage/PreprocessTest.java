package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import com.hartwig.pipeline.calling.structural.gridss.CommonEntities;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.command.ExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.calling.structural.gridss.command.SambambaGridssSortCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;

import org.junit.Before;
import org.junit.Test;

public class PreprocessTest implements CommonEntities {
    private String collectMetricsAndExtractReadsBam;
    private String computeSamTagsBam;

    private String extractSvReadsBashCommands;
    private String computeSamTagsBashCommands;
    private String clipsBashCommands;
    private String collectMetricsBaseOutputFilename;
    private String collectMetricsBashCommands;
    private String sortByDefaultCommands;
    private String sortByNameCommands;

    private CollectGridssMetrics collectGridssMetrics;
    private ComputeSamTags computeSamTags;
    private SoftClipsToSplitReads.ForPreprocess clips;
    private ExtractSvReads extractSvReads;
    private CommandFactory factory;
    private GridssToBashCommandConverter converter;
    private Preprocess.PreprocessResult result;
    private SambambaGridssSortCommand sortByDefault;
    private SambambaGridssSortCommand sortByName;

    @Before
    public void setup() {
        collectMetricsAndExtractReadsBam = REFERENCE_BAM + ".collected";
        collectMetricsBaseOutputFilename = REFERENCE_BAM + "_metrics";

        computeSamTagsBam = collectMetricsAndExtractReadsBam + ".computed";

        factory = mock(CommandFactory.class);
        converter = mock(GridssToBashCommandConverter.class);

        collectGridssMetrics = mock(CollectGridssMetrics.class);
        collectMetricsBashCommands = "collect metrics bash commands";
        when(factory.buildCollectGridssMetrics(any())).thenReturn(collectGridssMetrics);
        when(collectGridssMetrics.outputBaseFilename()).thenReturn(collectMetricsBaseOutputFilename);
        JavaClassCommand collectMetricsBash = mock(JavaClassCommand.class);
        when(converter.convert(collectGridssMetrics)).thenReturn(collectMetricsBash);
        when(collectMetricsBash.asBash()).thenReturn(collectMetricsBashCommands);

        extractSvReads = mock(ExtractSvReads.class);
        when(factory.buildExtractSvReads(any(), any(), any(), any())).thenReturn(extractSvReads);
        when(extractSvReads.resultantMetrics()).thenReturn(collectMetricsBaseOutputFilename);
        when(extractSvReads.resultantBam()).thenReturn(collectMetricsAndExtractReadsBam);

        computeSamTags = mock(ComputeSamTags.class);
        when(factory.buildComputeSamTags(any(), any(), any())).thenReturn(computeSamTags);
        when(computeSamTags.resultantBam()).thenReturn(computeSamTagsBam);

        clips = mock(SoftClipsToSplitReads.ForPreprocess.class);
        when(factory.buildSoftClipsToSplitReadsForPreProcess(any(), any(), any())).thenReturn(clips);

        extractSvReadsBashCommands = "extract sv reads bash commands";
        sortByDefault = mock(SambambaGridssSortCommand.class);
        sortByName = mock(SambambaGridssSortCommand.class);
        when(factory.buildSambambaCommandSortByDefault(any())).thenReturn(sortByDefault);
        when(factory.buildSambambaCommandSortByName(any())).thenReturn(sortByName);
        sortByDefaultCommands = "sort by default";
        when(sortByDefault.asBash()).thenReturn(sortByDefaultCommands);
        sortByNameCommands = "sorting by name";
        when(sortByName.asBash()).thenReturn(sortByNameCommands);

        JavaClassCommand collectorBash = mock(JavaClassCommand.class);
        when(converter.convert(extractSvReads)).thenReturn(collectorBash);
        when(collectorBash.asBash()).thenReturn(extractSvReadsBashCommands);

        computeSamTagsBashCommands = "compute sam tags bash commands";
        JavaClassCommand computeSamTagsBash = mock(JavaClassCommand.class);
        when(converter.convert(computeSamTags)).thenReturn(computeSamTagsBash);
        when(computeSamTagsBash.asBash()).thenReturn(computeSamTagsBashCommands);

        clipsBashCommands = "clips bash commands";
        JavaClassCommand clipsBash = mock(JavaClassCommand.class);
        when(converter.convert(clips)).thenReturn(clipsBash);
        when(clipsBash.asBash()).thenReturn(clipsBashCommands);

        result = new Preprocess(factory, converter).initialise(REFERENCE_BAM,
                REFERENCE_SAMPLE, REFERENCE_GENOME, OUTPUT_BAM, "working");
    }

    @Test
    public void shouldSetBamInResult() {
        assertThat(result.svBam()).isEqualTo(OUTPUT_BAM);
    }

    @Test
    public void shouldSetMetricsInResult() {
        assertThat(result.metrics()).isEqualTo(collectMetricsBaseOutputFilename);
    }

    @Test
    public void shouldSetBashCommandInResultToConcatenationOfBashFromEachCommandInOrder() {
        String stageTwo = format("(%s | %s)", extractSvReadsBashCommands, sortByNameCommands);
        String stageThree = format("(%s | %s)", computeSamTagsBashCommands, sortByDefaultCommands);

        List<BashCommand> generatedCommands = result.commands();
        assertThat(generatedCommands).isNotEmpty();
        assertThat(generatedCommands.size()).isEqualTo(4);
        assertThat(generatedCommands.get(0).asBash()).isEqualTo(collectMetricsBashCommands);
        assertThat(generatedCommands.get(1).asBash()).isEqualTo(stageTwo);
        assertThat(generatedCommands.get(2).asBash()).isEqualTo(stageThree);
        assertThat(generatedCommands.get(3).asBash()).isEqualTo(clipsBashCommands);
    }
}