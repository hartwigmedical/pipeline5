package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSbpRunResult.class)
@JsonSerialize(as = ImmutableSbpRunResult.class)
public interface SbpRunResult {

    @Value.Parameter
    String category();

    @Value.Parameter
    String type();

    static SbpRunResult of(final String category, final String type) {
        return ImmutableSbpRunResult.of(category, type);
    }

    static SbpRunResult from(final PipelineStatus status) {
        switch (status) {
            case QC_FAILED:
                return of("QCFailure", "HealthChecker");
            case FAILED:
                return of("TechnicalFailure", "Pipeline");
            case SUCCESS:
                return of("Success", "Success");
            default:
                throw new IllegalArgumentException("Status [{}] is not a valid final status for a production run. "
                        + "Check the PipelineState code to see how this state is propagated");
        }
    }
}
