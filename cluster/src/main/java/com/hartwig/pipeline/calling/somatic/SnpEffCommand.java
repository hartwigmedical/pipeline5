package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

class SnpEffCommand extends VersionedToolCommand {

    private static final String VERSION = "4.3s";

    SnpEffCommand(final String config, final String inputVcf, final String outputVcf) {
        super("snpEff",
                "snpEff.sh",
                VERSION,
                VmDirectories.TOOLS + "/snpEff/" + VERSION + "/snpEff.jar",
                config,
                "GRCh37.75",
                inputVcf,
                outputVcf);
    }
}
