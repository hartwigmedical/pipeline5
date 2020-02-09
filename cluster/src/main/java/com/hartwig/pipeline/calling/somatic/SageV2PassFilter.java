package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SageV2PassFilter extends SubStage {
    private final String tumorName;

    SageV2PassFilter(String tumorName) {
        super("sage.pass", OutputFile.GZIPPED_VCF);
        this.tumorName = tumorName;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex().includeHardPass().selectSample(tumorName).build();
    }
}
