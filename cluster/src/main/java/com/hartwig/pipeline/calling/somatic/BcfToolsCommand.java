package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.tools.Versions;

class BcfToolsCommand extends VersionedToolCommand {

    BcfToolsCommand(String... arguments) {
        super("bcftools", "bcftools", Versions.BCF_TOOLS, arguments);
    }
}
