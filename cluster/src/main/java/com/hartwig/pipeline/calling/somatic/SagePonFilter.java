package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.execution.vm.BashStartupScript;

class SagePonFilter extends SubStage {
    SagePonFilter() {
        super("sage.pon.filter", OutputFile.GZIPPED_VCF);
    }

    @Override
    BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new BcfToolsExcludeFilterCommand("'SAGE_PON_COUNT!=\".\" && MIN(SAGE_PON_COUNT) > 0'",
                "SAGE_PON",
                input.path(),
                output.path())).addCommand(new TabixCommand(output.path()));
    }
}
