package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class GenotypeGVCFs extends SubStage {

    private final String referenceFasta;

    GenotypeGVCFs(final String referenceFasta) {
        super("genotype_vcfs", OutputFile.VCF);
        this.referenceFasta = referenceFasta;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new GatkCommand(GermlineCaller.TOOL_HEAP,
                "GenotypeGVCFs",
                "-V",
                input.path(),
                "-R",
                referenceFasta,
                "-o",
                output.path()));
    }
}
