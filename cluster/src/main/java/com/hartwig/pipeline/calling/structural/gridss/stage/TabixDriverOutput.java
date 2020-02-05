package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class TabixDriverOutput extends SubStage {

    public TabixDriverOutput() {
        super("gridss.unfiltered", OutputFile.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new TabixCommand(input.path()));
    }
}
