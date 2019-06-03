package com.hartwig.pipeline.calling.command;

import com.hartwig.pipeline.tools.Versions;

public class BcfToolsCommand extends VersionedToolCommand {

    public BcfToolsCommand(String... arguments) {
        super("bcftools", "bcftools", Versions.BCF_TOOLS, arguments);
    }
}
