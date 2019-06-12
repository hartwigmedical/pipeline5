package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.*;

public class CommandFactory {
    public AnnotateUntemplatedSequence buildAnnotateUntemplatedSequence(String annotatedVcf, String referenceGenome) {
        return new AnnotateUntemplatedSequence(annotatedVcf, referenceGenome);
    }

    public AnnotateVariants buildAnnotateVariants(String sampleBam, String tumorBam, String assemblyBam, String rawVcf, String referenceGenome) {
        return new AnnotateVariants(sampleBam, tumorBam, assemblyBam, rawVcf, referenceGenome);
    }

    public AssembleBreakends buildAssembleBreakends(String sampleBam, String tumorBam, String referenceGenome) {
        return new AssembleBreakends(sampleBam, tumorBam, referenceGenome);
    }

    public BgzipCommand buildBgzipCommand(String annotatedVcf) {
        return new BgzipCommand(annotatedVcf);
    }

    public CollectGridssMetrics buildCollectGridssMetrics(String assembledBam) {
        return new CollectGridssMetrics(assembledBam);
    }

    public CollectGridssMetricsAndExtractSvReads buildCollectGridssMetricsAndExtractSvReads(String inputBam, String sampleName) {
        return new CollectGridssMetricsAndExtractSvReads(inputBam, sampleName);
    }

    public ComputeSamTags buildComputeSamTags(String inProgressBam, String referenceGenome, String sampleName) {
        return new ComputeSamTags(inProgressBam, referenceGenome, sampleName);
    }

    public IdentifyVariants buildIdentifyVariants(String sampleBam, String tumorBam, String assemblyBam, String referenceGenome, String blacklist) {
        return new IdentifyVariants(sampleBam, tumorBam, assemblyBam, referenceGenome);
    }

    public SoftClipsToSplitReads.ForAssemble buildSoftClipsToSplitReadsForAssemble(String intermediateBamPath, String referenceGenome, String outputBam) {
        return new SoftClipsToSplitReads.ForAssemble(intermediateBamPath, referenceGenome, outputBam);
    }

    public SoftClipsToSplitReads.ForPreprocess buildSoftClipsToSplitReadsForPreProcess(String intermediateBamPath, String referenceGenome, String outputBam) {
        return new SoftClipsToSplitReads.ForPreprocess(intermediateBamPath, referenceGenome, outputBam);
    }

    public TabixCommand buildTabixCommand(String inputVcf) {
        return new TabixCommand(inputVcf);
    }
}
