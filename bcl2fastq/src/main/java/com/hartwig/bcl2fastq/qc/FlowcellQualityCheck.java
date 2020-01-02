package com.hartwig.bcl2fastq.qc;

import com.hartwig.bcl2fastq.stats.Stats;

public interface FlowcellQualityCheck {

    QualityControlResult apply(Stats stats, String log);
}
