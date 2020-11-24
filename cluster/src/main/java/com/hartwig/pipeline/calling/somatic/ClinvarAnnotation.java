package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

class ClinvarAnnotation extends SubStage {

    private final ResourceFiles resourceFiles;

    ClinvarAnnotation(final ResourceFiles resourceFiles) {
        super("clinvar", FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .addAnnotation(resourceFiles.clinvarVcf(), "INFO/CLNSIG", "INFO/CLNSIGCONF")
                .build();
    }
}
