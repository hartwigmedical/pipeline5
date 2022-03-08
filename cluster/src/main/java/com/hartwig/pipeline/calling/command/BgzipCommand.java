package com.hartwig.pipeline.calling.command;

import com.hartwig.pipeline.tools.Versions;

public class BgzipCommand extends VersionedToolCommand {

    public BgzipCommand() {
        super("tabix", "bgzip", Versions.TABIX);
    }

    public BgzipCommand(final String vcf) {
        super("tabix", "bgzip", Versions.TABIX, "-f", vcf);
    }
}
