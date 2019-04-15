package com.hartwig.pipeline.calling.germline;

class GatkHaplotypeCaller {
    private final String jar;
    private final String inputBam;
    private final String referenceFasta;
    private final String outputVcf;

    GatkHaplotypeCaller(String jar, String inputBam, String referenceFasta, String outputVcf) {
        this.jar = jar;
        this.inputBam = inputBam;
        this.referenceFasta = referenceFasta;
        this.outputVcf = outputVcf;
    }

    String buildCommand() {
        String className = "org.broadinstitute.hellbender.Main HaplotypeCaller";
        return String.format("java -cp %s %s --input %s --output %s --reference %s",
                jar, className, inputBam, outputVcf, referenceFasta);
    }
}
