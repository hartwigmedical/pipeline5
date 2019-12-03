package com.hartwig.bcl2fastq.qc;

import java.util.Map;

public interface FastqQualityCheck {

    Map<String, QualityControlResult> apply(Stats stats);

}
