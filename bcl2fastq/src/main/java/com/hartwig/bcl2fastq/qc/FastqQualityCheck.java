package com.hartwig.bcl2fastq.qc;

import java.util.Map;

import com.hartwig.bcl2fastq.FastqId;

public interface FastqQualityCheck {

    Map<FastqId, QualityControlResult> apply(Stats stats);

}
