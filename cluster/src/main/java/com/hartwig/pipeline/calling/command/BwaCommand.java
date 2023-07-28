package com.hartwig.pipeline.calling.command;

import static com.hartwig.pipeline.tools.ExternalTool.BWA;

public class BwaCommand extends VersionedToolCommand {
    public BwaCommand(final String... arguments) {
        super(BWA.getToolName(), BWA.getBinary(), BWA.getVersion(), arguments);
    }
}
