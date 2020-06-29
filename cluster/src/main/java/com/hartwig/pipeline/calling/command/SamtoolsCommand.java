package com.hartwig.pipeline.calling.command;

import com.hartwig.pipeline.tools.Versions;

public class SamtoolsCommand extends VersionedToolCommand {
    public SamtoolsCommand(final String... arguments) {
        super("samtools", "samtools", Versions.SAMTOOLS, arguments);
    }
}