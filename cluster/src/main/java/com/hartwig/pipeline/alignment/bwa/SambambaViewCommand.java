package com.hartwig.pipeline.alignment.bwa;

import com.hartwig.pipeline.execution.vm.SambambaCommand;

class SambambaViewCommand extends SambambaCommand {

    SambambaViewCommand() {
        super("view", "-f", "bam", "-S", "-l0", "/dev/stdin");
    }
}
