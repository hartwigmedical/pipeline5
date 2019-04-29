package com.hartwig.pipeline.calling.somatic;

import static com.google.common.collect.Lists.newArrayList;

class SagePonAnnotation extends BcfToolsAnnotation {

    SagePonAnnotation(String pon) {
        super("sage.hotspots.pon", newArrayList(pon, "-c", "SAGE_PON_COUNT"));
    }
}
