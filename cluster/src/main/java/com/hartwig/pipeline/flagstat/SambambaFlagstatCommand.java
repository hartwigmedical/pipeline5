package com.hartwig.pipeline.flagstat;

import static com.hartwig.pipeline.tools.ExternalTool.SAMBAMBA;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;

class SambambaFlagstatCommand extends VersionedToolCommand {

    SambambaFlagstatCommand(final String bamLocation, final String flagstatFile) {
        super(SAMBAMBA.ToolName, SAMBAMBA.Binary, SAMBAMBA.Version, "flagstat", "-t", Bash.allCpus(), bamLocation, ">", flagstatFile);
    }
}