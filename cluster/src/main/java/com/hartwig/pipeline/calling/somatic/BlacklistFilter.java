package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

class BlacklistFilter extends SubStage {

    public static final String BLACKLIST_FILTER = "BLACKLIST";

    BlacklistFilter() {
        super("blacklist.filter", FileTypes.GZIPPED_VCF);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        final String expression = BlacklistBedAnnotation.BLACKLIST_BED_FLAG + "=1 || " + BlacklistVcfAnnotation.BLACKLIST_VCF_FLAG + "=1";
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .excludeSoftFilter(expression, BLACKLIST_FILTER)
                .build();
    }
}
