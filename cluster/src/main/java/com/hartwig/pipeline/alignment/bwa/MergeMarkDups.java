package com.hartwig.pipeline.alignment.bwa;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class MergeMarkDups extends SubStage {

    private final List<String> inputBamPaths;

    MergeMarkDups(final List<String> inputBamPaths) {
        super("", FileTypes.BAM);
        this.inputBamPaths = inputBamPaths;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new SambambaMarkdupCommand(inputBamPaths, output.path()));
    }
}
