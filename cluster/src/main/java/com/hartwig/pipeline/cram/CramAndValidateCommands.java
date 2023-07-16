package com.hartwig.pipeline.cram;

import static com.hartwig.pipeline.tools.ExternalTool.BAMCOMP;
import static com.hartwig.pipeline.tools.ExternalTool.SAMBAMBA;
import static com.hartwig.pipeline.tools.ExternalTool.SAMTOOLS;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.resource.ResourceFiles;

public class CramAndValidateCommands {
    private final String inputBam;
    private final String outputCram;

    private final ResourceFiles resourceFiles;

    public CramAndValidateCommands(final String inputBam, final String outputCram, final ResourceFiles resourceFiles) {
        this.inputBam = inputBam;
        this.outputCram = outputCram;
        this.resourceFiles = resourceFiles;
    }

    public List<BashCommand> commands() {
        return ImmutableList.of(new VersionedToolCommand(SAMTOOLS.ToolName,
                        SAMTOOLS.Binary,
                        SAMTOOLS.Version,
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
                new VersionedToolCommand(SAMTOOLS.ToolName,
                        SAMTOOLS.Binary,
                        SAMTOOLS.Version,
                        "reheader",
                        "--no-PG",
                        "--in-place",
                        "--command",
                        "'grep -v ^@PG'",
                        outputCram),
                new VersionedToolCommand(SAMTOOLS.ToolName,
                        SAMTOOLS.Binary,
                        SAMTOOLS.Version, "index", outputCram),
                new JavaClassCommand(BAMCOMP.ToolName,
                        BAMCOMP.Version,
                        BAMCOMP.Binary,
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
                        SAMTOOLS.binaryPath(),
                        "--sambamba-binary",
                        SAMBAMBA.binaryPath()));
    }
}
