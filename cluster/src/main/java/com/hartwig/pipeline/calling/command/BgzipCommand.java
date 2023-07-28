package com.hartwig.pipeline.calling.command;

import static com.hartwig.pipeline.tools.ExternalTool.TABIX;

public class BgzipCommand extends VersionedToolCommand {

    public BgzipCommand(final String vcf) {
        super(TABIX.getToolName(), "bgzip", TABIX.getVersion(), "-f", vcf);
    }
}
