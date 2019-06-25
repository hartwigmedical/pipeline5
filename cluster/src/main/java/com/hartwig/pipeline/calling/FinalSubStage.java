package com.hartwig.pipeline.calling;

import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class FinalSubStage extends SubStage{

    private final SubStage decorated;

    private FinalSubStage(final SubStage decorated) {
        super(decorated.getStageName(), decorated.getFileOutputType(), true);
        this.decorated = decorated;
    }

    public static FinalSubStage of(final SubStage decorated) {
        return new FinalSubStage(decorated);
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return decorated.bash(input, output, bash);
    }
}
