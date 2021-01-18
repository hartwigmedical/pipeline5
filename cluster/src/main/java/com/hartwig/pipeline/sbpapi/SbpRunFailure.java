package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.hartwig.pipeline.execution.PipelineStatus;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSbpRunFailure.class)
@JsonSerialize(as = ImmutableSbpRunFailure.class)
public interface SbpRunFailure {

    @Value.Parameter
    String category();

    @Value.Parameter
    String type();

    static SbpRunFailure of(final String category, final String type) {
        return ImmutableSbpRunFailure.of(category, type);
    }

    static SbpRunFailure from(final PipelineStatus status) {
        switch (status) {
            case QC_FAILED:
                return of("QCFailure", "HealthCheck");
            case FAILED:
                return of("TechnicalFailure", "Pipeline");
            default:
                throw new IllegalArgumentException(String.format("Status [%s] is not a valid final status for a production run. "
                        + "Check the PipelineState code to see how this state is propagated", status));
        }
    }
}