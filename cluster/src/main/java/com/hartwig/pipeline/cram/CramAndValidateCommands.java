package com.hartwig.pipeline.cram;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.JavaClassCommand;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.tools.Versions;

import java.util.Collections;
import java.util.List;

public class CramAndValidateCommands {
    private final String inputBam;
    private final String outputCram;

    public CramAndValidateCommands(String inputBam, String outputCram) {
        this.inputBam = inputBam;
        this.outputCram = outputCram;
    }

    public List<BashCommand> commands() {
        return ImmutableList.of(
                new VersionedToolCommand("samtools",
                        "samtools",
                        Versions.SAMTOOLS,
                        "view",
                        "-T",
                        Resource.REFERENCE_GENOME_FASTA,
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
