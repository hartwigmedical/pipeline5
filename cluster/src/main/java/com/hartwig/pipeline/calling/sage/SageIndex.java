package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.command.BgzipCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class SageIndex extends SubStage {

    public SageIndex() {
        super("sage.index", FileTypes.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        String beforeZip = output.path().replace(".gz", "");

        return ImmutableList.of(new BgzipCommand(beforeZip),
                new TabixCommand(output.path()));
    }
}
