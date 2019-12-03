package com.hartwig.bcl2fastq.qc;

import java.util.Map;

public interface SampleQualityCheck {

    Map<String, QualityControlResult> apply(Stats stats);
}
