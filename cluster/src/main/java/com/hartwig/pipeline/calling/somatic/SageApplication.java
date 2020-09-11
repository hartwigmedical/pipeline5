package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class SageApplication extends SubStage {

    private final SageCommandBuilder sageCommandBuilder;

    public SageApplication(final SageCommandBuilder sageCommandBuilder) {
        super("sage.somatic", FileTypes.GZIPPED_VCF);
        this.sageCommandBuilder = sageCommandBuilder;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return ImmutableList.of(sageCommandBuilder.build(output.path()));
    }
}
