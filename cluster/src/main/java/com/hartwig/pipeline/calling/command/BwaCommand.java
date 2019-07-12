package com.hartwig.pipeline.calling.command;

import com.hartwig.pipeline.tools.Versions;

public class BwaCommand extends VersionedToolCommand {
    public BwaCommand(final String... arguments) {
        super("bwa", "bwa", Versions.BWA, arguments);
    }
}
