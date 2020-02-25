package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class PonAnnotation extends SubStage {

    private final String pon;
    private final String type;

    PonAnnotation(final String name, final String pon, final String type) {
        super(name + ".annotated", OutputFile.GZIPPED_VCF);
        this.pon = pon;
        this.type = type;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex().addAnnotation(pon, type).build();
    }
}
