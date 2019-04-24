package com.hartwig.pipeline.calling.somatic;

class SageApplicationCommand extends SageCommand {
    SageApplicationCommand(final String tumorSampleName, final String tumorBamPath, final String referenceSampleName,
            final String referenceBamPath, final String knownHotspots, final String codingRegions, final String referenceGenomePath,
            final String outputVcf) {
        super("com.hartwig.hmftools.sage.SageHotspotApplication",
                "-tumor",
                tumorSampleName,
                "-tumor_bam",
                tumorBamPath,
                "-reference",
                referenceSampleName,
                "-reference_bam",
                referenceBamPath,
                "-known_hotspots",
                knownHotspots,
                "-coding_regions",
                codingRegions,
                "-ref_genome",
                referenceGenomePath,
                "-out",
                outputVcf);
    }
}
