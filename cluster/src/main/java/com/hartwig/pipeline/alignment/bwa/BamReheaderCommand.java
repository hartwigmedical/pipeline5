package com.hartwig.pipeline.alignment.bwa;

import com.hartwig.pipeline.calling.command.SamtoolsCommand;

class BamReheaderCommand extends SamtoolsCommand {
    BamReheaderCommand(final String inputBamPath) {
        super("reheader", "--no-PG", "--command", "'grep -v ^@PG'", inputBamPath);
    }
}
