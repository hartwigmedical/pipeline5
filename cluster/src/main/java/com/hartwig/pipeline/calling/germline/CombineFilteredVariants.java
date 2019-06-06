package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class CombineFilteredVariants extends SubStage {

    private final String otherVcfPath;

    CombineFilteredVariants(final String otherVcfPath) {
        super("filtered_variants", OutputFile.VCF);
        this.otherVcfPath = otherVcfPath;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new GatkCommand("10G",
                "CombineVariants",
                "-V",
                input.path(),
                "-V",
                otherVcfPath,
                "-o",
                output.path(),
                "--assumeIdenticalSamples",
                "true"));
    }
}
