package com.hartwig.pipeline.calling.command;

import static com.hartwig.pipeline.tools.ExternalTool.TABIX;

public class TabixCommand extends VersionedToolCommand {

    public TabixCommand(final String vcf) {
        super(TABIX.getToolName(), TABIX.getBinary(), TABIX.getVersion(), vcf, "-p", "vcf");
    }
}
