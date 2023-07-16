package com.hartwig.pipeline.calling.command;

import static com.hartwig.pipeline.tools.ExternalTool.BWA;

public class BwaCommand extends VersionedToolCommand {
    public BwaCommand(final String... arguments) {
        super(BWA.ToolName, BWA.Binary, BWA.Version, arguments);
    }
}
