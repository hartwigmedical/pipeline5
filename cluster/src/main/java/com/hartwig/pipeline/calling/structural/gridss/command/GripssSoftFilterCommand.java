package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.resource.ResourceFiles;

public class GripssSoftFilterCommand extends GripssCommand {

    public GripssSoftFilterCommand(final ResourceFiles resourceFiles, final String inputVcf, final String outputVcf) {
        super("com.hartwig.hmftools.gripss.GripssApplicationKt",
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-breakpoint_hotspot",
                resourceFiles.knownFusionPairBedpe(),
                "-breakend_pon",
                resourceFiles.gridssBreakendPon(),
                "-breakpoint_pon",
                resourceFiles.gridssBreakpointPon(),
                "-input_vcf",
                inputVcf,
                "-output_vcf",
                outputVcf);
    }
}
