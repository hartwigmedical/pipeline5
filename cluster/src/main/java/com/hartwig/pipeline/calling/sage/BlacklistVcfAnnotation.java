package com.hartwig.pipeline.calling.sage;

import static com.hartwig.pipeline.calling.sage.SageGermlinePostProcess.SAGE_GERMLINE_FILTERED;

import java.util.List;

import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

class BlacklistVcfAnnotation extends SubStage {

    public static final String BLACKLIST_VCF_FLAG = "BLACKLIST_VCF";

    private final ResourceFiles resourceFiles;

    BlacklistVcfAnnotation(final ResourceFiles resourceFiles) {
        super(SAGE_GERMLINE_FILTERED, FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .addAnnotationWithFlag(resourceFiles.sageGermlineBlacklistVcf(), BLACKLIST_VCF_FLAG)
                .build();
    }

}
