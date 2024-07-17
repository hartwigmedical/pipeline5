package com.hartwig.pipeline.stages;

import java.util.List;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.execution.OutputFile;

public abstract class SubStage implements Function<SubStageInputOutput, SubStageInputOutput> {

    private final String stageName;
    private final String fileOutputType;

    public SubStage(final String stageName, final String fileOutputType) {
        this.stageName = stageName;
        this.fileOutputType = fileOutputType;
    }

    String getStageName() {
        return stageName;
    }

    String getFileOutputType() {
        return fileOutputType;
    }

    @Override
    public SubStageInputOutput apply(final SubStageInputOutput input) {
        OutputFile outputFile = stageName.isEmpty()
                ? OutputFile.of(input.sampleName(), fileOutputType)
                : OutputFile.of(input.sampleName(), stageName, fileOutputType);
        return SubStageInputOutput.of(input.sampleName(), outputFile, combine(input.bash(), bash(input.outputFile(), outputFile)));
    }

    private static List<BashCommand> combine(final List<BashCommand> prior, final List<BashCommand> next) {
        ImmutableList.Builder<BashCommand> listBuilder = ImmutableList.builder();
        listBuilder.addAll(prior);
        listBuilder.addAll(next);
        return listBuilder.build();
    }

    public abstract List<BashCommand> bash(final OutputFile input, OutputFile output);
}
