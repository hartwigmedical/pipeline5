package com.hartwig.pipeline.alignment.bwa;

class SambambaSortCommand extends SambambaCommand {

    SambambaSortCommand(final String outputFileName, final String inputFileName) {
        super("sort", "-o", outputFileName, inputFileName);
    }
}
