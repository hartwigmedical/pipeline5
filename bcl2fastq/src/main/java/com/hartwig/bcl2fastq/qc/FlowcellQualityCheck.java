package com.hartwig.bcl2fastq.qc;

public interface FlowcellQualityCheck {

    QualityControlResult apply(Stats stats, String log);
}
