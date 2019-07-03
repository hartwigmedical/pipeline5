package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.calling.structural.gridss.GridssCommon;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class AnnotateVariants implements GridssCommand {
    private final String sampleBam;
    private final String tumorBam;
    private final String assemblyBam;
    private final String inputVcf;
    private final String outputVcf;
    private final String referenceGenome;

    public AnnotateVariants(final String referenceBam, final String tumorBam, final String assemblyBam, final String inputVcf,
            final String referenceGenome, final String tumorSampleName) {
        this.sampleBam = referenceBam;
        this.tumorBam = tumorBam;
        this.assemblyBam = assemblyBam;
        this.inputVcf = inputVcf;
        this.referenceGenome = referenceGenome;
        this.outputVcf = VmDirectories.outputFile(tumorSampleName + ".annotated_variants.vcf");
    }

    public String resultantVcf() {
        return outputVcf;
    }

    @Override
    public String className() {
        return "gridss.AnnotateVariants";
    }

    @Override
    public String arguments() {
        return new GridssArguments().add("tmp_dir", "/tmp")
                .add("working_dir", VmDirectories.OUTPUT)
                .add("reference_sequence", referenceGenome)
                .add("input", sampleBam)
                .add("input", tumorBam)
                .add("input_vcf", inputVcf)
                .add("output_vcf", resultantVcf())
                .add("assembly", assemblyBam)
                .add("blacklist", GridssCommon.blacklist())
                .add("configuration_file", GridssCommon.configFile())
                .asBash();
    }
}
