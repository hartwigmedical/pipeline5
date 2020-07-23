package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class PonFilter extends SubStage {
    PonFilter() {
        super("sage.pon.filter", OutputFile.GZIPPED_VCF);
    }

    private static final String PON_FILTER = "PON";
    private static final String PANEL = "INFO/TIER=\"PANEL\" && PON_MAX>=5 && PON_COUNT >= 6";
    private static final String OTHER = "INFO/TIER!=\"HOTSPOT\" && INFO/TIER!=\"PANEL\" && PON_COUNT >= 6";
    private static final String HOTSPOT = "INFO/TIER=\"HOTSPOT\" && PON_MAX>=5 && PON_COUNT >= 10";

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .excludeSoftFilter("'PON_COUNT!=\".\" && " + HOTSPOT + "'", PON_FILTER)
                .excludeSoftFilter("'PON_COUNT!=\".\" && " + PANEL + "'", PON_FILTER)
                .excludeSoftFilter("'PON_COUNT!=\".\" && " + OTHER + "'", PON_FILTER)
                .build();
    }
}
