package com.hartwig.bcl2fastq.qc;

public interface QualityCheck {

    QualityControlResult apply(Stats stats, String log);
}
