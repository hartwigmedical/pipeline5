package com.hartwig.pipeline.flagstat;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.tools.Versions;

class SambambaFlagstatCommand extends VersionedToolCommand {

    SambambaFlagstatCommand(String bamLocation, String flagstatFile, int numThreads) {
        super("sambamba", "sambamba", Versions.SAMBAMBA, "flagstat", "-t", String.valueOf(numThreads), bamLocation, ">", flagstatFile);
    }
}