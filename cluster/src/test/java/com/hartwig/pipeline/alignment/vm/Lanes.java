package com.hartwig.pipeline.alignment.vm;

import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.Lane;

class Lanes {

    static ImmutableLane.Builder emptyBuilder() {
        return Lane.builder().firstOfPairPath("").secondOfPairPath("").flowCellId("");
    }
}
