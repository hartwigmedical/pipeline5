package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class SageApplication extends SubStage {

    private final SageCommandBuilder sageCommandBuilder;

    public static final String SAGE_SOMATIC_VCF_ID = "sage.somatic";
    public static final String SAGE_GERMLINE_VCF_ID = "sage.germline";

    public SageApplication(final SageCommandBuilder sageCommandBuilder) {
        super(sageCommandBuilder.isSomatic() ? SAGE_SOMATIC_VCF_ID : SAGE_GERMLINE_VCF_ID, FileTypes.GZIPPED_VCF);
        this.sageCommandBuilder = sageCommandBuilder;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return sageCommandBuilder.build(output.path());
    }
}
