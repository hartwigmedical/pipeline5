package com.hartwig.pipeline.calling;

import java.util.function.Function;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

public abstract class SubStage implements Function<SubStageInputOutput, SubStageInputOutput> {

    private final String stageName;
    private final String fileOutputType;

    public SubStage(final String stageName, final String fileOutputType) {
        this.stageName = stageName;
        this.fileOutputType = fileOutputType;
    }

    @Override
    public SubStageInputOutput apply(final SubStageInputOutput input) {
        OutputFile outputFile = OutputFile.of(input.sampleName(), stageName, fileOutputType);
        return SubStageInputOutput.of(input.sampleName(), outputFile, bash(input.outputFile(), outputFile, input.currentBash()));
    }

    public abstract BashStartupScript bash(final OutputFile input, OutputFile output, final BashStartupScript bash);
}
