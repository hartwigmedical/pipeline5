package com.hartwig.pipeline.cram;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.SambambaCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.tools.Versions;

public class CramAndValidateCommands {
    private final String inputBam;
    private final String outputCram;

    private final ResourceFiles resourceFiles;

    public CramAndValidateCommands(String inputBam, String outputCram, ResourceFiles resourceFiles) {
        this.inputBam = inputBam;
        this.outputCram = outputCram;
        this.resourceFiles = resourceFiles;
    }

    public List<BashCommand> commands() {
        return ImmutableList.of(new VersionedToolCommand("samtools",
                        "samtools",
                        Versions.SAMTOOLS,
                        "view",
                        "-T",
                        resourceFiles.refGenomeFile(),
                        "-o",
                        outputCram,
                        "-O",
                        "cram,embed_ref=1",
                        "-@",
                        Bash.allCpus(),
                        inputBam),
                new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, "index", outputCram),
                new JavaClassCommand("bamcomp",
                        Versions.BAMCOMP,
                        "bamcomp.jar",
                        "com.hartwig.bamcomp.BamCompMain",
                        "4G",
                        Collections.emptyList(),
                        "-r",
                        resourceFiles.refGenomeFile(),
                        "-1",
                        inputBam,
                        "-2",
                        outputCram,
                        "-n",
                        String.valueOf(CramConversion.NUMBER_OF_CORES),
                        "--samtools-binary",
                        path(Versions.SAMTOOLS, SamtoolsCommand.SAMTOOLS),
                        "--sambamba-binary",
                        path(Versions.SAMBAMBA, SambambaCommand.SAMBAMBA)));
    }

    private static String path(final String version, final String tool) {
        return String.format("%s/%s/%s/%s", VmDirectories.TOOLS, tool, version, tool);
    }
}
