package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class SageV2PonFilter extends SubStage {
    public SageV2PonFilter() {
        super("sage.pon.filter", OutputFile.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .excludeSoftFilter("'PON_COUNT!= \".\" && (MIN(PON_COUNT) > 9 || (MIN(PON_COUNT) > 2 && INFO/TIER!=\"HOTSPOT\"))'",
                        "SAGE_PON")
                .build();
    }
}
