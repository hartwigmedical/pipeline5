package com.hartwig.pipeline.calling.sage;

import com.hartwig.computeengine.execution.vm.OutputFile;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.stages.SubStage;

import java.util.List;

class PassFilter extends SubStage {

    PassFilter() {
        super("sage.pass", FileTypes.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex().includeHardPass().build();
    }
}
