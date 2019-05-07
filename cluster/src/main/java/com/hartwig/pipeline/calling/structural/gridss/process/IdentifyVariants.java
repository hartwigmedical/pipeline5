package com.hartwig.pipeline.calling.structural.gridss.process;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import static java.util.Arrays.asList;

public class IdentifyVariants implements BashCommand {
    private final String sampleBam;
    private final String tumorBam;
    private final String assemblyBam;
    private final String referenceGenome;
    private final String blacklist;

    public IdentifyVariants(String sampleBam, String tumorBam, String assemblyBam, String referenceGenome, String blacklist) {
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = assemblyBam;
        this.referenceGenome = referenceGenome;
        this.blacklist = blacklist;
    }

    @Override
    public String asBash() {
        return GridssCommon.gridssCommand("gridss.IdentifyVariants", "8G",
                asList("-Dgridss.output_to_temp_file=true"), new GridssArguments()
                .add("tmp_dir", "/tmp")
                .add("working_dir", VmDirectories.OUTPUT)
                .add("reference_sequence", referenceGenome)
                .add("input", sampleBam)
                .add("input", tumorBam)
                .add("output_vcf", resultantVcf())
                .add("assembly", assemblyBam)
                .add("worker_threads", "16")
                .add("blacklist", blacklist)
                .add("configuration_file", GridssCommon.configFile())
                .asBash()
        ).asBash();
    }

    public String resultantVcf() {
        return VmDirectories.outputFile("sv_calling.vcf");
    }
}
