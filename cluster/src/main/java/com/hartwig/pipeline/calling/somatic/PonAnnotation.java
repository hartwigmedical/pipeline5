package com.hartwig.pipeline.calling.somatic;

import static com.google.common.collect.Lists.newArrayList;

class PonAnnotation extends BcfToolsAnnotation {

    PonAnnotation(final String name, final String pon, final String type) {
        super(name, newArrayList(pon, "-c", type));
    }
}
