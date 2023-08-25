package com.hartwig.pipeline.calling.germline;

import com.hartwig.computeengine.execution.vm.OutputFile;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.gatk.GatkCommand;
import com.hartwig.pipeline.stages.SubStage;

import java.util.Collections;
import java.util.List;

public class CombineFilteredVariants extends SubStage {

    private final String otherVcfPath;
    private final String referenceFasta;

    CombineFilteredVariants(final String otherVcfPath, final String referenceFasta) {
        super("filtered_variants", FileTypes.VCF);
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
