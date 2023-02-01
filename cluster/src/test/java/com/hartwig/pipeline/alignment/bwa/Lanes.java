package com.hartwig.pipeline.alignment.bwa;

import com.hartwig.pdl.ImmutableLaneInput;
import com.hartwig.pdl.LaneInput;

class Lanes {

    static ImmutableLaneInput.Builder emptyBuilder() {
        return LaneInput.builder().firstOfPairPath("").secondOfPairPath("").flowCellId("");
    }
}
