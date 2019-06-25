package com.hartwig.pipeline.calling;

import java.util.function.Function;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

public abstract class SubStage implements Function<SubStageInputOutput, SubStageInputOutput> {

    private final String stageName;
    private final String fileOutputType;
    private final boolean isFinalSubStage;

    public SubStage(final String stageName, final String fileOutputType) {
        this(stageName, fileOutputType, false);
    }

    public SubStage(final String stageName, final String fileOutputType, final boolean isFinalSubStage) {
        this.stageName = stageName;
        this.fileOutputType = fileOutputType;
        this.isFinalSubStage = isFinalSubStage;
    }

    String getStageName() {
        return stageName;
    }

    String getFileOutputType() {
        return fileOutputType;
    }

    @Override
    public SubStageInputOutput apply(final SubStageInputOutput input) {
        OutputFile outputFile = OutputFile.of(input.sampleName(), stageName, fileOutputType, isFinalSubStage);
        return SubStageInputOutput.of(input.sampleName(), outputFile, bash(input.outputFile(), outputFile, input.currentBash()));
    }

    public abstract BashStartupScript bash(final OutputFile input, OutputFile output, final BashStartupScript bash);
}
