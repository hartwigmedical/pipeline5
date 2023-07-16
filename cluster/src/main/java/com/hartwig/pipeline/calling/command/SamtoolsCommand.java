package com.hartwig.pipeline.calling.command;

import static java.lang.String.format;

import static com.hartwig.pipeline.tools.ExternalTool.SAMTOOLS;

import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.resource.ResourceFiles;

public class SamtoolsCommand extends VersionedToolCommand {

    private static final int SORT_MEMORY_PER_CORE = 2;

    public SamtoolsCommand(final String... arguments) {
        super(SAMTOOLS.ToolName, SAMTOOLS.Binary, SAMTOOLS.Version, arguments);
    }

    public static SamtoolsCommand index(final String file) {
        return new SamtoolsCommand("index", "-@", Bash.allCpus(), file);
    }

    public static SamtoolsCommand sort(final String inputFile, final String outputFile) {

        String arguments = format("sort -@ %s -m %dG -T tmp -O bam %s -o %s",
                Bash.allCpus(), SORT_MEMORY_PER_CORE, inputFile, outputFile);

        return new SamtoolsCommand(arguments);
    }

    public static SamtoolsCommand toUncompressedBam(final ResourceFiles resourceFiles, final String inputFile, final String outputFile) {
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

    public static SamtoolsCommand sliceToUncompressedBam(final ResourceFiles resourceFiles, final String bedFile, final String inputFile,
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