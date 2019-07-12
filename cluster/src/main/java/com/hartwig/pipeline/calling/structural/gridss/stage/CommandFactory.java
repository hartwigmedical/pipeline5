package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateUntemplatedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateVariants;
import com.hartwig.pipeline.calling.structural.gridss.command.AssembleBreakends;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetrics;
import com.hartwig.pipeline.calling.structural.gridss.command.ComputeSamTags;
import com.hartwig.pipeline.calling.structural.gridss.command.ExtractSvReads;
import com.hartwig.pipeline.calling.structural.gridss.command.IdentifyVariants;
import com.hartwig.pipeline.calling.structural.gridss.command.SambambaGridssSortCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;

public class CommandFactory {
    public AnnotateUntemplatedSequence buildAnnotateUntemplatedSequence(final String annotatedVcf, final String referenceGenome,
            final String jointName) {
        return new AnnotateUntemplatedSequence(annotatedVcf, referenceGenome, jointName);
    }

    public AnnotateVariants buildAnnotateVariants(final String sampleBam, final String tumorBam, final String assemblyBam,
            final String rawVcf, final String referenceGenome, final String jointName) {
        return new AnnotateVariants(sampleBam, tumorBam, assemblyBam, rawVcf, referenceGenome, jointName);
    }

    public AssembleBreakends buildAssembleBreakends(final String sampleBam, final String tumorBam, final String referenceGenome,
            final String jointName) {
        return new AssembleBreakends(sampleBam, tumorBam, referenceGenome, jointName);
    }

    public BgzipCommand buildBgzipCommand(final String annotatedVcf) {
        return new BgzipCommand(annotatedVcf);
    }

    public CollectGridssMetrics buildCollectGridssMetrics(final String inputBam) {
        return new CollectGridssMetrics(inputBam);
    }

    public ExtractSvReads buildExtractSvReads(final String inputBam, final String sampleName, final String insertSizeMetrics,
            final String workingDirectory) {
        return new ExtractSvReads(inputBam, sampleName, insertSizeMetrics, workingDirectory);
    }

    public ComputeSamTags buildComputeSamTags(final String inProgressBam, final String referenceGenome, final String sampleName) {
        return new ComputeSamTags(inProgressBam, referenceGenome, sampleName);
    }

    public IdentifyVariants buildIdentifyVariants(final String sampleBam, final String tumorBam, final String assemblyBam,
            final String referenceGenome) {
        return new IdentifyVariants(sampleBam, tumorBam, assemblyBam, referenceGenome);
    }

    public SoftClipsToSplitReads.ForAssemble buildSoftClipsToSplitReadsForAssemble(final String intermediateBamPath,
            final String referenceGenome, final String outputBam) {
        return new SoftClipsToSplitReads.ForAssemble(intermediateBamPath, referenceGenome, outputBam);
    }

    public SoftClipsToSplitReads.ForPreprocess buildSoftClipsToSplitReadsForPreProcess(final String intermediateBamPath,
            final String referenceGenome, final String outputBam) {
        return new SoftClipsToSplitReads.ForPreprocess(intermediateBamPath, referenceGenome, outputBam);
    }

    public TabixCommand buildTabixCommand(final String inputVcf) {
        return new TabixCommand(inputVcf);
    }

    public SambambaGridssSortCommand buildSambambaCommandSortByName(String outputBam) {
        return SambambaGridssSortCommand.sortByName(outputBam);
    }

    public SambambaGridssSortCommand buildSambambaCommandSortByDefault(String outputBam) {
        return SambambaGridssSortCommand.sortByDefault(outputBam);
    }
}
