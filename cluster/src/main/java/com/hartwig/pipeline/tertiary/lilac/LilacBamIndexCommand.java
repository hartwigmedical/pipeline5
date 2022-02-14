package com.hartwig.pipeline.tertiary.lilac;

import com.hartwig.pipeline.execution.vm.SambambaCommand;

public class LilacBamIndexCommand extends SambambaCommand {
    LilacBamIndexCommand(final String inputBam) {
        super("index", inputBam);
    }
}
