package com.hartwig.pipeline.alignment.bwa;

import com.hartwig.pipeline.calling.command.SamtoolsCommand;

class SamtoolsReheaderCommand extends SamtoolsCommand {
    SamtoolsReheaderCommand(final String outputBamPath) {
        super("reheader", "--no-PG", "--command", "'grep -v ^@PG'", "/dev/stdin", ">", outputBamPath);
    }
}
