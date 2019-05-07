package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.process.*;

public class CommandFactory {
    public AnnotateUntemplatedSequence buildAnnotateUntemplatedSequence(String annotatedVcf, String referenceGenome) {
        return new AnnotateUntemplatedSequence(annotatedVcf, referenceGenome);
    }

    public AnnotateVariants buildAnnotateVariants(String sampleBam, String tumorBam, String rawVcf) {
        return new AnnotateVariants();
    }

    public AssembleBreakends buildAssembleBreakends(String sampleBam, String tumorBam, String referenceGenome, String blacklist) {
        return new AssembleBreakends(sampleBam, tumorBam, referenceGenome, blacklist);
    }

    public BgzipCommand buildBgzipCommand(String annotatedVcf) {
        return new BgzipCommand(annotatedVcf);
    }

    public CollectGridssMetrics buildCollectGridssMetrics(String assembledBam) {
        return new CollectGridssMetrics(assembledBam);
    }

    public CollectGridssMetricsAndExtractSvReads buildCollectGridssMetricsAndExtractSvReads(String inputBam) {
        return new CollectGridssMetricsAndExtractSvReads(inputBam, "");
    }

    public ComputeSamTags buildComputeSamTags(String inProgressBam, String referenceGenome) {
        return new ComputeSamTags(inProgressBam, referenceGenome);
    }

    public IdentifyVariants buildIdentifyVariants(String sampleBam, String tumorBam, String assemblyBam, String referenceGenome, String blacklist) {
        return new IdentifyVariants(sampleBam, tumorBam, assemblyBam, referenceGenome, blacklist);
    }

    public SoftClipsToSplitReads.ForAssemble buildSoftClipsToSplitReadsForAssemble(String intermediateBamPath, String referenceGenome) {
        return new SoftClipsToSplitReads.ForAssemble(intermediateBamPath, referenceGenome);
    }

    public SoftClipsToSplitReads.ForPreprocess buildSoftClipsToSplitReadsForPreProcess(String intermediateBamPath, String referenceGenome) {
        return new SoftClipsToSplitReads.ForPreprocess(intermediateBamPath, referenceGenome);
    }

    public TabixCommand buildTabixCommand(String inputVcf) {
        return new TabixCommand(inputVcf);
    }
}
