package com.hartwig.pipeline.calling.command;

import com.hartwig.computeengine.execution.vm.Bash;

import static com.hartwig.pipeline.tools.ExternalTool.SAMTOOLS;
import static java.lang.String.format;

public class SamtoolsCommand extends VersionedToolCommand {

    private static final int SORT_MEMORY_PER_CORE = 2;

    public SamtoolsCommand(final String... arguments) {
        super(SAMTOOLS.getToolName(), SAMTOOLS.getBinary(), SAMTOOLS.getVersion(), arguments);
    }

    public static SamtoolsCommand index(final String file) {
        return new SamtoolsCommand("index", "-@", Bash.allCpus(), file);
    }

    public static SamtoolsCommand sort(final String inputFile, final String outputFile) {

        String arguments = format("sort -@ %s -m %dG -T tmp -O bam %s -o %s", Bash.allCpus(), SORT_MEMORY_PER_CORE, inputFile, outputFile);

        return new SamtoolsCommand(arguments);
    }
}