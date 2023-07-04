package com.hartwig.pipeline.alignment.bwa;

import com.hartwig.pipeline.execution.vm.SambambaCommand;

class SambambaSortCommand extends SambambaCommand {

    SambambaSortCommand(final String outputFileName, final String inputFileName) {
        super("sort", "-o", outputFileName, inputFileName);
    }
}
