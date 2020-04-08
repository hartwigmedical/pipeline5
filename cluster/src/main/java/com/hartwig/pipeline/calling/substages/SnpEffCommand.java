package com.hartwig.pipeline.calling.substages;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

class SnpEffCommand extends VersionedToolCommand {

    SnpEffCommand(final String config, final String inputVcf, final String outputVcf, final String refGenomeVersion) {
        super("snpEff",
                "snpEff.sh",
                Versions.SNPEFF,
                VmDirectories.TOOLS + "/snpEff/" + Versions.SNPEFF + "/snpEff.jar",
                config,
                refGenomeVersion,
                inputVcf,
                outputVcf);
    }
}
