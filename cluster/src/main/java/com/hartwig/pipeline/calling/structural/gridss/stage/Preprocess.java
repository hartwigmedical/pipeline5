package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;

import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.command.ExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;
import com.hartwig.pipeline.execution.vm.unix.SubShellCommand;

import org.immutables.value.Value;

public class Preprocess {
    private final CommandFactory factory;

    @Value.Immutable
    public interface PreprocessResult {
        String svBam();

        String metrics();

        List<BashCommand> commands();
    }

    public Preprocess(final CommandFactory factory) {
        this.factory = factory;
    }

    public PreprocessResult initialise(final String inputBam, final String sampleName, final String referenceGenome,
            final String workingDirectory, final String outputSvBam) {
        CollectGridssMetrics gridssCollectMetrics = factory.buildCollectGridssMetrics(inputBam, workingDirectory);
        ExtractSvReads extractSvReads = factory.buildExtractSvReads(inputBam,
                sampleName,
                format("%s.insert_size_metrics", gridssCollectMetrics.outputBaseFilename()));
        SubShellCommand secondSubStage = new SubShellCommand(new PipeCommands(extractSvReads,
                factory.buildSambambaCommandSortByName(extractSvReads.resultantBam())));
        ComputeSamTags gridssComputeSamTags = factory.buildComputeSamTags(extractSvReads.resultantBam(), referenceGenome, sampleName);
        SubShellCommand thirdSubStage = new SubShellCommand(new PipeCommands(gridssComputeSamTags,
                factory.buildSambambaCommandSortByDefault(gridssComputeSamTags.resultantBam())));
        SoftClipsToSplitReads.ForPreprocess softClips =
                factory.buildSoftClipsToSplitReadsForPreProcess(gridssComputeSamTags.resultantBam(), referenceGenome, outputSvBam);

        return ImmutablePreprocessResult.builder()
                .svBam(outputSvBam)
                .metrics(gridssCollectMetrics.outputBaseFilename())
                .commands(asList(gridssCollectMetrics, secondSubStage, thirdSubStage, softClips))
                .build();

    }
}
