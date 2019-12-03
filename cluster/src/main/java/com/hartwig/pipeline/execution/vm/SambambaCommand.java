package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.tools.Versions;

public class SambambaCommand extends VersionedToolCommand {

    public SambambaCommand(String... arguments) {
        super("sambamba", "sambamba", Versions.SAMBAMBA, arguments);
    }
}