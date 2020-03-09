package com.hartwig.bcl2fastq;

import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.tools.Versions;

class Bcl2FastqCommand extends VersionedToolCommand {

    Bcl2FastqCommand(final String workingDirectory, final String outputDirectory, final int totalCpus) {
        super("bcl2fastq",
                "bcl2fastq",
                Versions.BCL2FASTQ,
                "-R",
                workingDirectory,
                "-o",
                outputDirectory,
                "--reports-dir",
                outputDirectory + "/Reports",
                "-r",
                String.valueOf(totalCpus / 2),
                "-w",
                String.valueOf(totalCpus / 2),
                "-p",
                String.valueOf(totalCpus));
    }
}