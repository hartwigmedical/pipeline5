package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.structural.gridss.process.CollectGridssMetricsAndExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.process.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.process.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashCommand;
import org.immutables.value.Value;

import java.util.List;

import static java.util.Arrays.asList;

public class Preprocess {
    private CommandFactory factory;

    @Value.Immutable
    public interface PreprocessResult {
        String svBam();
        String metrics();
        List<BashCommand> commands();
    }

    public Preprocess(CommandFactory factory) {
        this.factory = factory;
    }

    public PreprocessResult initialise(String inputBam, String referenceGenome, String insertSizeMetrics) {
        CollectGridssMetricsAndExtractSvReads collector = factory.buildCollectGridssMetricsAndExtractSvReads(inputBam);
        ComputeSamTags computeSamTags = factory.buildComputeSamTags(collector.resultantBam(), referenceGenome);
        SoftClipsToSplitReads.ForPreprocess softClips = factory.buildSoftClipsToSplitReadsForPreProcess(computeSamTags.resultantBam(), referenceGenome);

        return ImmutablePreprocessResult.builder().svBam(softClips.resultantBam())
                .metrics(collector.resultantMetrics())
                .commands(asList(collector, computeSamTags, softClips))
                .build();
    }
}
