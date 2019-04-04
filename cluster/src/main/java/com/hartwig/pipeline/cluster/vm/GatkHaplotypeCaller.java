package com.hartwig.pipeline.cluster.vm;

public class GatkHaplotypeCaller {
    private final String jar;
    private final String inputBam;
    private final String referenceFasta;
    private final String outputVcf;

    public GatkHaplotypeCaller(String jar, String inputBam, String referenceFasta, String outputVcf) {
        this.jar = jar;
        this.inputBam = inputBam;
        this.referenceFasta = referenceFasta;
        this.outputVcf = outputVcf;
    }

    public String buildCommand() {
        String className = "org.broadinstitute.hellbender.Main HaplotypeCaller";
        return String.format("java -cp %s %s --input %s --output %s --reference %s",
                jar, className, inputBam, outputVcf, referenceFasta);
    }
}
