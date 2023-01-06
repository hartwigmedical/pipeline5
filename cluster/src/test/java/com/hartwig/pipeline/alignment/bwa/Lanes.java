package com.hartwig.pipeline.alignment.bwa;

import com.hartwig.pipeline.input.ImmutableLane;
import com.hartwig.pipeline.input.Lane;

class Lanes {

    static ImmutableLane.Builder emptyBuilder() {
        return Lane.builder().firstOfPairPath("").secondOfPairPath("").flowCellId("");
    }
}
