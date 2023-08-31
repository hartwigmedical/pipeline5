package com.hartwig.pipeline;

public enum PipelineStatus {
    SUCCESS,
    FAILED,
    QC_FAILED,
    SKIPPED,
    PERSISTED,
    PROVIDED,
    PREEMPTED,
    UNKNOWN
}