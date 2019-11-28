package com.hartwig.bcl2fastq.qc;

public class ErrorsInLog implements QualityCheck {
    @Override
    public QualityControlResult apply(final Stats stats, final String log) {
        return QualityControlResult.of(log.contains("with 0 errors and"));
    }
}
