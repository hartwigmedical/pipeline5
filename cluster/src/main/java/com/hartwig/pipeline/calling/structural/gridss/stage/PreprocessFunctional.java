package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.CollectGridssMetricsSubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class PreprocessFunctional extends SubStage {
    private final String inputBam;
    private final String sampleName;
    private final String referenceGenome;
    private final String workingDirectory;
    private final String outputSvBam;

    public PreprocessFunctional(final String inputBam, final String sampleName, final String referenceGenome,
            final String workingDirectory, final String outputSvBam) {
        super("preprocess", "bam");
        this.inputBam = inputBam;
        this.sampleName = sampleName;
        this.referenceGenome = referenceGenome;
        this.workingDirectory = workingDirectory;
        this.outputSvBam = outputSvBam;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        new CollectGridssMetricsSubStage(inputBam, workingDirectory);
        //        .andThen(new ExtractSvReads(...))
        //        ...
        //        .bash();
        return null;
    }
}
