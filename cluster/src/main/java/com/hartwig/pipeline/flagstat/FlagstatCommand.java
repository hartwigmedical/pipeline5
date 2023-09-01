package com.hartwig.pipeline.flagstat;

import static com.hartwig.pipeline.tools.ExternalTool.SAMTOOLS;

import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;

class FlagstatCommand extends VersionedToolCommand {

    FlagstatCommand(final String bamLocation, final String flagstatFile) {
        super(SAMTOOLS.getToolName(),
                SAMTOOLS.getBinary(),
                SAMTOOLS.getVersion(),
                "flagstat",
                "-@",
                Bash.allCpus(),
                bamLocation,
                ">",
                flagstatFile);
    }
}