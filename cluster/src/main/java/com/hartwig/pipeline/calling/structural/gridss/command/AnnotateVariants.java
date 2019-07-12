package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.util.Arrays.asList;

import java.util.List;

import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AnnotateVariants extends GridssCommand {
    private final String sampleBam;
    private final String tumorBam;
    private final String assemblyBam;
    private final String inputVcf;
    private final String outputVcf;
    private final String configurationFile;
    private final String blacklist;
    private final String referenceGenome;

    public AnnotateVariants(final String referenceBam, final String tumorBam, final String assemblyBam, final String inputVcf,
            final String referenceGenome, final String tumorSampleName, String configurationFile, String blacklist) {
        this.sampleBam = referenceBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = assemblyBam;
        this.inputVcf = inputVcf;
        this.referenceGenome = referenceGenome;
        this.outputVcf = VmDirectories.outputFile(tumorSampleName + ".annotated_variants.vcf");
        this.configurationFile = configurationFile;
        this.blacklist = blacklist;
    }

    public String resultantVcf() {
        return outputVcf;
    }

    @Override
    public String className() {
        return "gridss.AnnotateVariants";
    }

    @Override
    public List<GridssArgument> arguments() {
        return asList(GridssArgument.tempDir(),
                new GridssArgument("working_dir", VmDirectories.OUTPUT),
                new GridssArgument("reference_sequence", referenceGenome),
                new GridssArgument("input", sampleBam),
                new GridssArgument("input", tumorBam),
                new GridssArgument("input_vcf", inputVcf),
                new GridssArgument("output_vcf", resultantVcf()),
                new GridssArgument("assembly", assemblyBam),
                GridssArgument.blacklist(blacklist),
                GridssArgument.configFile(configurationFile));
    }
}
