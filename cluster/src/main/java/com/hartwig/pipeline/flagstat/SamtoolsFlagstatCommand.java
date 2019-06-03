package com.hartwig.pipeline.flagstat;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.tools.Versions;

class SamtoolsFlagstatCommand extends VersionedToolCommand {

    SamtoolsFlagstatCommand(String bamLocation, String flagstatFile) {
        super("samtools", "samtools", Versions.SAMTOOLS, "flagstat", bamLocation, ">", flagstatFile);
    }
}