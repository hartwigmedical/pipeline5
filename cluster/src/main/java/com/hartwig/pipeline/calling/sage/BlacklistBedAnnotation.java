package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

class BlacklistBedAnnotation extends SubStage {

    public static final String BLACKLIST_BED_FLAG = "BLACKLIST_BED";

    private final ResourceFiles resourceFiles;

    BlacklistBedAnnotation(final ResourceFiles resourceFiles) {
        super("blacklist.regions", FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .addAnnotationWithFlag(resourceFiles.sageGermlineBlacklistBed(), BLACKLIST_BED_FLAG, "CHROM", "FROM", "TO")
                .build();
    }

}
