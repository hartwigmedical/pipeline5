package com.hartwig.pipeline.execution.vm;

import static com.hartwig.pipeline.tools.ExternalTool.SAMBAMBA;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;

public class SambambaCommand extends VersionedToolCommand {

    public SambambaCommand(final String... arguments) {
        super(SAMBAMBA.getToolName(), SAMBAMBA.getBinary(), SAMBAMBA.getVersion(), arguments);
    }
}