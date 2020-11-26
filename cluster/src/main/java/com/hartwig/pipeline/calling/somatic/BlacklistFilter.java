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
        final String expression =
                String.format("%s=1 || %s=1", BlacklistBedAnnotation.BLACKLIST_BED_FLAG, BlacklistVcfAnnotation.BLACKLIST_VCF_FLAG);
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .excludeSoftFilter(expression, BLACKLIST_FILTER)
                .build();
    }
}
