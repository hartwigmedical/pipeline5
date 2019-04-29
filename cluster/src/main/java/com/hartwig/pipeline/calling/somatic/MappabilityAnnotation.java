package com.hartwig.pipeline.calling.somatic;

import static com.google.common.collect.Lists.newArrayList;

class MappabilityAnnotation extends BcfToolsAnnotation {

    MappabilityAnnotation(final String bed, final String hdr) {
        super("mappability", newArrayList(bed, "-h", hdr, "-c", "CHROM,FROM,TO,-,MAPPABILITY"));
    }
}
