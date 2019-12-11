package com.hartwig.bcl2fastq.qc;

import java.util.Map;

import com.hartwig.bcl2fastq.stats.Stats;

public interface SampleQualityCheck {

    Map<String, QualityControlResult> apply(Stats stats);
}
