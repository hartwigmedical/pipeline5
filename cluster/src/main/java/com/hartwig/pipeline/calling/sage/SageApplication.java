package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class SageApplication extends SubStage {

    private final SageCommandBuilder sageCommandBuilder;

    public SageApplication(final SageCommandBuilder sageCommandBuilder) {
        super(sageCommandBuilder.isSomatic() ? "sage.somatic" : "sage.germline", FileTypes.GZIPPED_VCF);
        this.sageCommandBuilder = sageCommandBuilder;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return sageCommandBuilder.build(output.path());
    }
}
