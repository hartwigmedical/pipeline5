package com.hartwig.pipeline.calling.somatic;

class BcfToolsCommand extends VersionedToolCommand {

    BcfToolsCommand(String... arguments) {
        super("bcftools", "bcftools", "1.3.1", arguments);
    }
}
