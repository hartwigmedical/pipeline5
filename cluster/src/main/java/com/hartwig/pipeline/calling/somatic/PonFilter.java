package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

class PonFilter extends SubStage {

    PonFilter() {
        super("pon.filtered", OutputFile.GZIPPED_VCF);
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new PipeCommands(new BcfToolsPipeableExcludeFilterCommand(
                "'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5'",
                "GERMLINE_PON",
                input.path()),
                new BcfToolsExcludeFilterCommand("'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3'", "SOMATIC_PON", output.path())))
                .addCommand(new TabixCommand(output.path()));
    }
}
