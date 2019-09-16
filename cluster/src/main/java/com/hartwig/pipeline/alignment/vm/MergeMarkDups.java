package com.hartwig.pipeline.alignment.vm;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class MergeMarkDups extends SubStage {

    private final List<String> inputBamPaths;

    MergeMarkDups(final List<String> inputBamPaths) {
        super("sorted", OutputFile.BAM);
        this.inputBamPaths = inputBamPaths;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new SambambaMarkdupCommand(inputBamPaths, output.path()));
    }
}
