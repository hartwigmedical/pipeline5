package com.hartwig.pipeline.calling.structural.gripss;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class GripssCommand extends JavaJarCommand
{
    public GripssCommand(
            final ResourceFiles resourceFiles, final String tumorSample, final String refSample, final String inputVcf) {
        super("gripss",
                Versions.GRIPSS,
                "gripss.jar",
                "16G",
                ImmutableList.<String>builder().add(
                "-ref_genome",
                resourceFiles.refGenomeFile(),
                "-known_hotspot_file",
                resourceFiles.knownFusionPairBedpe(),
                "-pon_sgl_file",
                resourceFiles.gridssBreakendPon(),
                "-pon_sv_file",
                resourceFiles.gridssBreakpointPon(),
                "-reference",
                refSample,
                "-sample",
                tumorSample,
                "-vcf",
                inputVcf,
                "-output_dir",
                VmDirectories.OUTPUT,
                "-output_id somatic").build());
    }
}
