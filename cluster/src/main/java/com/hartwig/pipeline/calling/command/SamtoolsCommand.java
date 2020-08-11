package com.hartwig.pipeline.calling.command;

import com.hartwig.pipeline.tools.Versions;

public class SamtoolsCommand extends VersionedToolCommand {

    public static final String SAMTOOLS = "samtools";

    public SamtoolsCommand(final String... arguments) {
        super(SAMTOOLS, SAMTOOLS, Versions.SAMTOOLS, arguments);
    }
}