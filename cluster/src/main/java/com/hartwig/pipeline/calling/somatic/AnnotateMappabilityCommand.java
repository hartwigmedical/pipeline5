package com.hartwig.pipeline.calling.somatic;

class AnnotateMappabilityCommand extends BcfToolsCommand {
    AnnotateMappabilityCommand(final String mappabilityBed, final String mappabilityHdr, final String inputVcf, final String outputVcf) {
        super("annotate",
                "-a",
                mappabilityBed,
                "-h",
                mappabilityHdr,
                "-c",
                "CHROM,FROM,TO,-,MAPPABILITY",
                "-o",
                outputVcf,
                "-O",
                "z",
                inputVcf);
    }
}
