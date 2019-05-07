package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;

class BcfToolsCommand extends VersionedToolCommand {

    BcfToolsCommand(String... arguments) {
        super("bcftools", "bcftools", "1.3.1", arguments);
    }
}
