package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.lang.String.format;

public class AssembleBreakends implements BashCommand {
    private static final int WORKER_THREADS = 2;

    private final String referenceGenome;
    private final String sampleBam;
    private final String tumorBam;
    private final String assemblyBam;

    public AssembleBreakends(String sampleBam, String tumorBam, String referenceGenome) {
        this.referenceGenome = referenceGenome;
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;

        this.assemblyBam = VmDirectories.outputFile(format("reference-tumor.assembly.bam", sampleBam));
    }

    @Override
    public String asBash() {
        return GridssCommon.gridssCommand("gridss.AssembleBreakends", "31G",
                new GridssArguments()
                        .addTempDir()
                        .add("working_dir", VmDirectories.OUTPUT)
                        .add("reference_sequence", referenceGenome)
                        .add("input", sampleBam)
                        .add("input", tumorBam)
                        .add("output", assemblyBam)
                        .add("worker_threads", String.valueOf(WORKER_THREADS))
                        .addBlacklist()
                        .addConfigFile()
                        .asBash()).asBash();
    }

    public String assemblyBam() {
        return assemblyBam;
    }
}
