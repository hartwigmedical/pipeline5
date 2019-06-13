package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.*;

public class CommandFactory {
    public AnnotateUntemplatedSequence buildAnnotateUntemplatedSequence(final String annotatedVcf,
                                                                        final String referenceGenome) {
        return new AnnotateUntemplatedSequence(annotatedVcf, referenceGenome);
    }

    public AnnotateVariants buildAnnotateVariants(final String sampleBam, final String tumorBam,
                                                  final String assemblyBam, final String rawVcf,
                                                  final String referenceGenome) {
        return new AnnotateVariants(sampleBam, tumorBam, assemblyBam, rawVcf, referenceGenome);
    }

    public AssembleBreakends buildAssembleBreakends(final String sampleBam, final String tumorBam,
                                                    final String referenceGenome) {
        return new AssembleBreakends(sampleBam, tumorBam, referenceGenome);
    }

    public BgzipCommand buildBgzipCommand(final String annotatedVcf) {
        return new BgzipCommand(annotatedVcf);
    }

    public CollectGridssMetrics buildCollectGridssMetrics(final String assembledBam) {
        return new CollectGridssMetrics(assembledBam);
    }

    public CollectGridssMetricsAndExtractSvReads buildCollectGridssMetricsAndExtractSvReads(final String inputBam,
                                                                                            final String sampleName) {
        return new CollectGridssMetricsAndExtractSvReads(inputBam, sampleName);
    }

    public ComputeSamTags buildComputeSamTags(final String inProgressBam, final String referenceGenome,
                                              final String sampleName) {
        return new ComputeSamTags(inProgressBam, referenceGenome, sampleName);
    }

    public IdentifyVariants buildIdentifyVariants(final String sampleBam, final String tumorBam,
                                                  final String assemblyBam, final String referenceGenome) {
        return new IdentifyVariants(sampleBam, tumorBam, assemblyBam, referenceGenome);
    }

    public SoftClipsToSplitReads.ForAssemble buildSoftClipsToSplitReadsForAssemble(final String intermediateBamPath,
                                                                                   final String referenceGenome,
                                                                                   final String outputBam) {
        return new SoftClipsToSplitReads.ForAssemble(intermediateBamPath, referenceGenome, outputBam);
    }

    public SoftClipsToSplitReads.ForPreprocess buildSoftClipsToSplitReadsForPreProcess(final String intermediateBamPath,
                                                                                       final String referenceGenome,
                                                                                       final String outputBam) {
        return new SoftClipsToSplitReads.ForPreprocess(intermediateBamPath, referenceGenome, outputBam);
    }

    public TabixCommand buildTabixCommand(final String inputVcf) {
        return new TabixCommand(inputVcf);
    }
}
