package com.hartwig.pipeline.calling.germline;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.command.BashCommand;
import com.hartwig.pipeline.gatk.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class GenotypeGVCFs extends SubStage {

    private final String referenceFasta;

    GenotypeGVCFs(final String referenceFasta) {
        super("genotype_vcfs", FileTypes.VCF);
        this.referenceFasta = referenceFasta;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new GatkCommand(GermlineCaller.TOOL_HEAP,
                "GenotypeGVCFs",
                "-V",
                input.path(),
                "-R",
                referenceFasta,
                "-o",
                output.path()));
    }
}
