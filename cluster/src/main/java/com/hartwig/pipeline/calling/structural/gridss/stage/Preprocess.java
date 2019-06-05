package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.somatic.PipeCommands;
import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetricsAndExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.command.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.command.GridssToBashCommandConverter;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.SubShellCommand;
import org.immutables.value.Value;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Preprocess {
    private CommandFactory factory;
    private GridssToBashCommandConverter converter;

    @Value.Immutable
    public interface PreprocessResult {
        String svBam();
        String metrics();
        List<BashCommand> commands();
    }

    public Preprocess(CommandFactory factory, GridssToBashCommandConverter converter) {
        this.factory = factory;
        this.converter = converter;
    }

    public PreprocessResult initialise(String inputBam, String sampleName, String referenceGenome, String insertSizeMetrics, String outputSvBam) {
        CollectGridssMetricsAndExtractSvReads gridssCollectMetrics = factory.buildCollectGridssMetricsAndExtractSvReads(inputBam, insertSizeMetrics, sampleName);
        SubShellCommand firstSubStage = new SubShellCommand(new PipeCommands(
                converter.convert(gridssCollectMetrics),
                () -> format("%s sort -O bam -T /tmp/samtools.sort.tmp -n -l 0 -@ 2 -o %s",
                        GridssCommon.pathToSamtools(), gridssCollectMetrics.resultantBam())));
        ComputeSamTags gridssComputeSamTags = factory.buildComputeSamTags(gridssCollectMetrics.resultantBam(), referenceGenome, sampleName);
        SubShellCommand secondSubStage = new SubShellCommand(new PipeCommands(
                converter.convert(gridssComputeSamTags),
                () -> format("%s sort -O bam -T /tmp/samtools.sort.tmp -@ 2 -o %s",
                        GridssCommon.pathToSamtools(), gridssComputeSamTags.resultantBam())));
        SoftClipsToSplitReads.ForPreprocess softClips = factory.buildSoftClipsToSplitReadsForPreProcess(gridssComputeSamTags.resultantBam(), referenceGenome, outputSvBam);

        return ImmutablePreprocessResult.builder().svBam(outputSvBam)
                .metrics(gridssCollectMetrics.resultantMetrics())
                .commands(asList(firstSubStage, secondSubStage, converter.convert(softClips)))
                .build();

    }
}
