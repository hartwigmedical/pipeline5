package com.hartwig.pipeline.flagstat;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.tools.Versions;

class SambambaFlagstatCommand extends VersionedToolCommand {

    SambambaFlagstatCommand(String bamLocation, String flagstatFile) {
        super("sambamba", "sambamba", Versions.SAMBAMBA, "flagstat", "-t", Bash.allCpus(), bamLocation, ">", flagstatFile);
    }
}