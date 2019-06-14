package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.calling.structural.gridss.command.*;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;
import org.immutables.value.Value;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Preprocess {
    private final CommandFactory factory;
    private final GridssToBashCommandConverter converter;

    @Value.Immutable
    public interface PreprocessResult {
        String svBam();
        String metrics();
        List<BashCommand> commands();
    }

    public Preprocess(final CommandFactory factory, final GridssToBashCommandConverter converter) {
        this.factory = factory;
        this.converter = converter;
    }

    public PreprocessResult initialise(final String inputBam, final String sampleName, final String referenceGenome,
                                       final String outputSvBam) {
        CollectGridssMetrics gridssCollectMetrics = factory.buildCollectGridssMetrics(inputBam);
        ExtractSvReads extractSvReads = factory.buildExtractSvReads(inputBam, sampleName,
                format("%s.insert_size_metrics", gridssCollectMetrics.outputBaseFilename()));
        SubShellCommand secondSubStage = new SubShellCommand(new PipeCommands(
                converter.convert(extractSvReads),
                () -> format("%s sort -O bam -T /tmp/samtools.sort.tmp -n -l 0 -@ 2 -o %s",
                        GridssCommon.pathToSamtools(), extractSvReads.resultantBam())));

        ComputeSamTags gridssComputeSamTags = factory.buildComputeSamTags(extractSvReads.resultantBam(), referenceGenome, sampleName);
        SubShellCommand thirdSubStage = new SubShellCommand(new PipeCommands(
                converter.convert(gridssComputeSamTags),
                () -> format("%s sort -O bam -T /tmp/samtools.sort.tmp -@ 2 -o %s",
                        GridssCommon.pathToSamtools(), gridssComputeSamTags.resultantBam())));
        SoftClipsToSplitReads.ForPreprocess softClips = factory.buildSoftClipsToSplitReadsForPreProcess(gridssComputeSamTags.resultantBam(), referenceGenome, outputSvBam);

        return ImmutablePreprocessResult.builder().svBam(outputSvBam)
                .metrics(gridssCollectMetrics.outputBaseFilename())
                .commands(asList(
                        converter.convert(gridssCollectMetrics),
                        secondSubStage,
                        thirdSubStage,
                        converter.convert(softClips)))
                .build();

    }
}
