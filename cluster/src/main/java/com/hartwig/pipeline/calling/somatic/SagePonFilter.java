package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SagePonFilter extends SubStage {
    SagePonFilter() {
        super("sage.pon.filter", OutputFile.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .excludeSoftFilter("'SAGE_PON_COUNT!=\".\" && MIN(SAGE_PON_COUNT) > 0'", "SAGE_PON")
                .build();
    }
}
