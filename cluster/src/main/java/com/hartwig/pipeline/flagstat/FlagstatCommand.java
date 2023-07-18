package com.hartwig.pipeline.flagstat;

import static com.hartwig.pipeline.tools.ExternalTool.SAMTOOLS;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;

class FlagstatCommand extends VersionedToolCommand {

    FlagstatCommand(final String bamLocation, final String flagstatFile) {
        super(SAMTOOLS.ToolName, SAMTOOLS.Binary, SAMTOOLS.Version, "flagstat", "-@", Bash.allCpus(), bamLocation, ">", flagstatFile);
    }
}