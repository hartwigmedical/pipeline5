package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class SageV2PonFilter extends SubStage {
    SageV2PonFilter() {
        super("sage.pon.filter", OutputFile.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandBuilder(input.path(), output.path())
                .excludeSoftFilter("'PON_COUNT!= \".\" && MIN(PON_COUNT) > 5'", "SAGE_PON")
                .buildAndIndex();
    }
}
