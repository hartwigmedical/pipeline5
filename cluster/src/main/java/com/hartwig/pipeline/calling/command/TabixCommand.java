package com.hartwig.pipeline.calling.command;

import com.hartwig.pipeline.tools.Versions;

public class TabixCommand extends VersionedToolCommand {

    public TabixCommand(final String vcf) {
        super("tabix","tabix", Versions.TABIX, vcf, "-p", "vcf");
    }
}
