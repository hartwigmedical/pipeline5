package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class PonFilter extends SubStage {

    PonFilter() {
        super("pon.filtered", OutputFile.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandBuilder(input.path(), output.path())
                .excludeSoftFilter("'GERMLINE_PON_COUNT!= \".\" && MIN(GERMLINE_PON_COUNT) > 5'", "GERMLINE_PON")
                .excludeSoftFilter("'SOMATIC_PON_COUNT!=\".\" && MIN(SOMATIC_PON_COUNT) > 3'", "SOMATIC_PON")
                .buildAndIndex();
    }
}
