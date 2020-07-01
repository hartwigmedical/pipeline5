package com.hartwig.pipeline.calling.somatic;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class SageV2PostProcess extends SubStage {

    private final String assembly;

    public SageV2PostProcess(final String assembly) {
        super("sage.somatic.filtered", OutputFile.GZIPPED_VCF);
        this.assembly = assembly;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new SageV2PostProcessCommand(assembly, input.path(), output.path()));
    }
}
