package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class GripssCommand extends JavaClassCommand {

    public GripssCommand(final ResourceFiles resourceFiles, final String inputVcf, final String outputVcf) {
        super("gripss",
                Versions.GRIPSS,
                "gripss.jar",
                "com.hartwig.hmftools.gripss.GripssApplicationKt",
                "24G",
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
