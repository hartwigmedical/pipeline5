package com.hartwig.pipeline.calling.sage;

import com.hartwig.computeengine.execution.vm.OutputFile;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.stages.SubStage;

import java.util.List;

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
