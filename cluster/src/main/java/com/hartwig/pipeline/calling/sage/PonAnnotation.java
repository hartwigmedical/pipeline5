package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

class PonAnnotation extends SubStage {

    private final String pon;
    private final String type;

    PonAnnotation(final String name, final String pon, final String... columns) {
        super(name + ".annotated", FileTypes.GZIPPED_VCF);
        this.pon = pon;
        this.type = String.join(",", columns);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex().addAnnotation(pon, type).build();
    }
}
