package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.GatkCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class GenotypeGVCFs extends SubStage {

    private final String referenceFasta;
    private final String dbSnps;

    GenotypeGVCFs(final String referenceFasta, final String dbSnps) {
        super("genotype_vcfs", OutputFile.VCF);
        this.referenceFasta = referenceFasta;
        this.dbSnps = dbSnps;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new GatkCommand(GermlineCaller.TOOL_HEAP,
                "GenotypeGVCFs",
                "-V",
                input.path(),
                "-R",
                referenceFasta,
                "-D",
                dbSnps,
                "-o",
                output.path()));
    }
}
