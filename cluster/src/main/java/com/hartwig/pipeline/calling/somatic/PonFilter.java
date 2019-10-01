package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.unix.PipeCommands;

class PonFilter extends SubStage {

    PonFilter() {
        super("pon.filtered", OutputFile.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return ImmutableList.of(new PipeCommands(new BcfToolsPipeableExcludeFilterCommand(
                        "'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5'",
                        "GERMLINE_PON",
                        input.path()),
                        new BcfToolsExcludeFilterCommand("'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3'", "SOMATIC_PON", output.path())),
                new TabixCommand(output.path()));
    }
}
