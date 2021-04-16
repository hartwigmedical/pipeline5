package com.hartwig.pipeline.calling.command;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class SamtoolsCommand extends VersionedToolCommand {

    public static final String SAMTOOLS = "samtools";

    public SamtoolsCommand(final String... arguments) {
        super(SAMTOOLS, SAMTOOLS, Versions.SAMTOOLS, arguments);
    }

    public static SamtoolsCommand index(final String file) {
        return new SamtoolsCommand("index", file);
    }

    public static SamtoolsCommand toUncompressedBam(ResourceFiles resourceFiles, final String inputFile, final String outputFile) {
        return new SamtoolsCommand("view",
                "-T",
                resourceFiles.refGenomeFile(),
                "-o",
                outputFile,
                "-u",
                "-@",
                Bash.allCpus(),
                "-M",
                inputFile);
    }

    public static SamtoolsCommand sliceToUncompressedBam(ResourceFiles resourceFiles, final String bedFile, final String inputFile,
            final String outputFile) {
        return new SamtoolsCommand("view",
                "-T",
                resourceFiles.refGenomeFile(),
                "-L",
                bedFile,
                "-o",
                outputFile,
                "-u",
                "-@",
                Bash.allCpus(),
                "-M",
                inputFile);
    }
}