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

    public String buildCommand() {
        return "java -jar " + jar + " --analysis_type HaplotypeCaller --input_file " + inputBam
                + " -o " + outputVcf +" --reference_sequence "+ referenceFasta
                + " -nct $(grep -c '^processor' /proc/cpuinfo) -variant_index_type LINEAR"
                + " -variant_index_parameter 128000 -stand_call_conf 15.0 -ERC GVCF"
                + " -GQB 5 -GQB 10 -GQB 15 -GQB 20 -GQB 30 -GQB 40 -GQB 50 -GQB 60 --sample_ploidy 2";
    }
}
