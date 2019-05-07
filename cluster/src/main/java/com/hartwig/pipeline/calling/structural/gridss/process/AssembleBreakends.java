package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AssembleBreakends implements BashCommand {
    private static final int WORKER_THREADS = 2;
    private static final String CONFIG_FILE = "/config_file";

    private final String referenceGenome;
    private final String sampleBam;
    private final String tumorBam;
    private final String outputBam;
    private final String blacklist;
    private final String outputConfig;

    public AssembleBreakends(String sampleBam, String tumorBam, String referenceGenome, String blacklist) {
        this.referenceGenome = referenceGenome;
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;
        this.blacklist = blacklist;

        this.outputBam = String.format("%s/assembled.bam", VmDirectories.OUTPUT);
        this.outputConfig = String.format("%s/gridss.config", VmDirectories.OUTPUT);
    }

    @Override
    public String asBash() {
        return GridssCommon.gridssCommand("gridss.AssembleBreakends", "31G",
                new GridssArguments()
                        .add("tmp_dir", "/tmp")
                        .add("working_dir", VmDirectories.OUTPUT)
                        .add("reference_sequence", referenceGenome)
                        .add("input", sampleBam)
                        .add("input", tumorBam)
                        .add("output", outputBam)
                        .add("worker_threads", String.valueOf(WORKER_THREADS))
                        .add("blacklist", blacklist)
                        .add("configuration_file", outputConfig)
                        .asBash()).asBash();
    }

    public String resultantBam() {
        return outputBam;
    }

    String resultantConfig() {
        return outputConfig;
    }
}
