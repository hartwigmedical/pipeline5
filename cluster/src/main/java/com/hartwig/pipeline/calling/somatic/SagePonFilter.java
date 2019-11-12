package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SagePonFilter extends SubStage {
    SagePonFilter() {
        super("sage.pon.filter", OutputFile.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return ImmutableList.of(new BcfToolsExcludeFilterCommand("'SAGE_PON_COUNT!=\".\" && MIN(SAGE_PON_COUNT) > 0'",
                "SAGE_PON",
                input.path(),
                output.path()), new TabixCommand(output.path()));
    }
}
