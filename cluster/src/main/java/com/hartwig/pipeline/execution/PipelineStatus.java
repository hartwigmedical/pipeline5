package com.hartwig.pipeline.execution;

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