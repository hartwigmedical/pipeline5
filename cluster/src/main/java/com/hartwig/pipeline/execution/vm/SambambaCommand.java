package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.tools.Versions;

public class SambambaCommand extends VersionedToolCommand {

    public static final String SAMBAMBA = "sambamba";

    public SambambaCommand(final String... arguments) {
        super(SAMBAMBA, SAMBAMBA, Versions.SAMBAMBA, arguments);
    }
}