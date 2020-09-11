package com.hartwig.pipeline.stages;

import java.util.List;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class FinalSubStage extends SubStage{

    private final SubStage decorated;

    private FinalSubStage(final SubStage decorated) {
        super(decorated.getStageName() + ".final", decorated.getFileOutputType());
        this.decorated = decorated;
    }

    public static FinalSubStage of(final SubStage decorated) {
        return new FinalSubStage(decorated);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return decorated.bash(input, output);
    }
}
