package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.resource.ResourceFiles;

public class GripssSoftFilterCommand extends GripssCommand {

    public GripssSoftFilterCommand(final ResourceFiles resourceFiles, final String tumorSample, final String refSample,
            final String inputVcf, final String outputVcf) {
        super("com.hartwig.hmftools.gripsskt.GripssApplicationKt",
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-breakpoint_hotspot",
                resourceFiles.knownFusionPairBedpe(),
                "-breakend_pon",
                resourceFiles.gridssBreakendPon(),
                "-breakpoint_pon",
                resourceFiles.gridssBreakpointPon(),
                "-reference",
                refSample,
                "-tumor",
                tumorSample,
                "-input_vcf",
                inputVcf,
                "-output_vcf",
                outputVcf,
                "-paired_normal_tumor_ordinals");
    }
}
