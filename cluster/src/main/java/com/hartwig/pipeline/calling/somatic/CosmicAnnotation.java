package com.hartwig.pipeline.calling.somatic;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

class CosmicAnnotation extends SubStage {

    private final String cosmicDB;

    CosmicAnnotation(final String cosmicDB) {
        super("cosmic.annotated", OutputFile.GZIPPED_VCF);
        this.cosmicDB = cosmicDB;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new BcfToolsAnnotationCommand(Lists.newArrayList(cosmicDB, "-c", "ID,INFO"), input.path(), output.path()))
                .addCommand(new TabixCommand(output.path()));
    }
}
