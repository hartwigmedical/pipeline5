package com.hartwig.pipeline.calling.command;

import static com.hartwig.pipeline.tools.ExternalTool.TABIX;

public class BgzipCommand extends VersionedToolCommand
{

    public BgzipCommand() {
        super(TABIX.ToolName,"bgzip",TABIX.Version);
    }

    public BgzipCommand(final String vcf) {
        super(TABIX.ToolName, "bgzip", TABIX.Version, "-f", vcf);
    }
}
