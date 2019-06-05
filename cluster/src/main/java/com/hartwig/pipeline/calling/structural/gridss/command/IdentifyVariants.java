package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class IdentifyVariants implements GridssCommand {
    private final String sampleBam;
    private final String tumorBam;
    private final String assemblyBam;
    private final String referenceGenome;

    public IdentifyVariants(String sampleBam, String tumorBam, String assemblyBam, String referenceGenome) {
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = assemblyBam;
        this.referenceGenome = referenceGenome;
    }

    @Override
    public String arguments() {
        return new GridssArguments()
                .add("tmp_dir", "/tmp")
                .add("working_dir", VmDirectories.OUTPUT)
                .add("reference_sequence", referenceGenome)
                .add("input", sampleBam)
                .add("input", tumorBam)
                .add("output_vcf", resultantVcf())
                .add("assembly", assemblyBam)
                .add("worker_threads", "16")
                .addBlacklist()
                .add("configuration_file", GridssCommon.configFile())
                .asBash();
    }

    public String resultantVcf() {
        return VmDirectories.outputFile("sv_calling.vcf");
    }

    @Override
    public String className() {
        return "gridss.IdentifyVariants";
    }
}
