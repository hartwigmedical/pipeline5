package com.hartwig.pipeline.calling.structural.gridss.command;

import java.util.Arrays;
import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class IdentifyVariants extends GridssCommand {
    private final String sampleBam;
    private final String tumorBam;
    private final String assemblyBam;
    private final String outputVcf;
    private final String referenceGenome;
    private final String configurationFile;
    private final String blacklist;

    public IdentifyVariants(final String sampleBam, final String tumorBam, final String assemblyBam, final String outputVcf,
                            final String referenceGenome, final String configurationFile, final String blacklist) {
        this.sampleBam = sampleBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = assemblyBam;
        this.outputVcf = outputVcf;
        this.referenceGenome = referenceGenome;
        this.configurationFile = configurationFile;
        this.blacklist = blacklist;
    }

    @Override
    public List<GridssArgument> arguments() {
        return Arrays.asList(new GridssArgument("working_dir", VmDirectories.OUTPUT),
                new GridssArgument("reference_sequence", referenceGenome),
                new GridssArgument("input", sampleBam),
                new GridssArgument("input", tumorBam),
                new GridssArgument("output_vcf", outputVcf),
                new GridssArgument("assembly", assemblyBam),
                GridssArgument.blacklist(blacklist),
                GridssArgument.configFile(configurationFile)
        );
    }

    @Override
    public String className() {
        return "gridss.IdentifyVariants";
    }
}
