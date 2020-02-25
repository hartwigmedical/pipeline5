package com.hartwig.pipeline.calling.somatic;

class SageHotspotAnnotationCommand extends SageCommand {
    SageHotspotAnnotationCommand(final String sourceVcf, final String hotspotVcf, final String knownHotspots, final String outputVcf) {
        super("com.hartwig.hmftools.sage.SageHotspotAnnotation", "8G", "-source_vcf",
                sourceVcf,
                "-hotspot_vcf",
                hotspotVcf,
                "-known_hotspots",
                knownHotspots,
                "-out",
                outputVcf);
    }
}
