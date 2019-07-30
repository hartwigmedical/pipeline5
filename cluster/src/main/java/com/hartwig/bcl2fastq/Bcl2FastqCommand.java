package com.hartwig.bcl2fastq;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.tools.Versions;

class Bcl2FastqCommand extends VersionedToolCommand {

    Bcl2FastqCommand(final String workingDirectory, final String outputDirectory) {
        super("bcl2fastq",
                "bcl2fastq",
                Versions.BCL2FASTQ,
                "-R",
                workingDirectory,
                "-o",
                outputDirectory,
                "--ignore-missing-bcls",
                "--ignore-missing-filter",
                "--ignore-missing-positions",
                "--ignore-missing-controls");
    }
}
