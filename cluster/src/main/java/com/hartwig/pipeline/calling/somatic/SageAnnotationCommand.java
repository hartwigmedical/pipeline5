package com.hartwig.pipeline.calling.somatic;

class SageAnnotationCommand extends SageCommand {
    SageAnnotationCommand(final String sourceVcf, final String hotspotVcf, final String knownHotspots, final String outputVcf) {
        super("com.hartwig.hmftools.sage.SageHotspotAnnotation",
                "-source_vcf",
                sourceVcf,
                "-hotspot_vcf",
                hotspotVcf,
                "-known_hotspots",
                knownHotspots,
                "-out",
                outputVcf);
    }
}
