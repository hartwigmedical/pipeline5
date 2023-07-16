package com.hartwig.pipeline.calling.command;

import static com.hartwig.pipeline.tools.ExternalTool.BCF_TOOLS;

public class BcfToolsCommand extends VersionedToolCommand {

    public BcfToolsCommand(final String... arguments) {
        super(BCF_TOOLS.ToolName, BCF_TOOLS.Binary, BCF_TOOLS.Version, arguments);
    }
}
