package com.hartwig.pipeline.alignment.vm;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.tools.Versions;

class SambambaCommand extends VersionedToolCommand {

    SambambaCommand(String ... arguments) {
        super("sambamba", "sambamba", Versions.SAMBAMBA, arguments);
    }
}