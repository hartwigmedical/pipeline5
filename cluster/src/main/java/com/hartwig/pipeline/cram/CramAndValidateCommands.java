package com.hartwig.pipeline.cram;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
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
        return ImmutableList.of(
                new VersionedToolCommand("samtools",
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
                new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS,
                        "index", outputCram),
                new JavaClassCommand("bamcomp", Versions.BAMCOMP, "bamcomp.jar",
                        "com.hartwig.bamcomp.BamToCramValidator", "4G",
                        Collections.emptyList(), inputBam, outputCram, String.valueOf(CramConversion.NUMBER_OF_CORES)));
    }
}
