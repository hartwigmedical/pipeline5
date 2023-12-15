package com.hartwig.pipeline;

import java.util.Objects;

import com.hartwig.computeengine.execution.ComputeEngineStatus;

public enum PipelineStatus {
    FAILED,
    PERSISTED,
    PREEMPTED,
    PROVIDED,
    QC_FAILED,
    SKIPPED,
    SUCCESS,
    UNKNOWN;

    public static PipelineStatus of(ComputeEngineStatus computeEngineStatus) {
        Objects.requireNonNull(computeEngineStatus, "Invalid conversion, computeEngineStatus was null");
        switch (computeEngineStatus) {
            case SUCCESS:
                return PipelineStatus.SUCCESS;
            case FAILED:
                return PipelineStatus.FAILED;
            case SKIPPED:
                return PipelineStatus.SKIPPED;
            case PREEMPTED:
                return PipelineStatus.PREEMPTED;
            default:
                throw new IllegalArgumentException(String.format("Compute engine state [%s] is not a valid pipeline state", computeEngineStatus));
        }
    }

}