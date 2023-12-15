package com.hartwig.pipeline.alignment.bwa;

import com.hartwig.pipeline.calling.command.SamtoolsCommand;

class SamtoolsViewCommand extends SamtoolsCommand {
    SamtoolsViewCommand() {
        super("view", "--no-PG", "--bam", "--uncompressed", "/dev/stdin");
    }
}
