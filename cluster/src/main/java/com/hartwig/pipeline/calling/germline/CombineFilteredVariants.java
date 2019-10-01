package com.hartwig.pipeline.calling.germline;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class CombineFilteredVariants extends SubStage {

    private final String otherVcfPath;
    private final String referenceFasta;

    CombineFilteredVariants(final String otherVcfPath, final String referenceFasta) {
        super("filtered_variants", OutputFile.VCF);
        this.otherVcfPath = otherVcfPath;
        this.referenceFasta = referenceFasta;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new GatkCommand(GermlineCaller.TOOL_HEAP,
                "CombineVariants",
                "-V",
                input.path(),
                "-V",
                otherVcfPath,
                "-o",
                output.path(),
                "-R",
                referenceFasta,
                "--assumeIdenticalSamples"));
    }
}
