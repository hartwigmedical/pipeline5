package com.hartwig.pipeline.adam;

import java.io.Serializable;

import org.immutables.value.Value;

@Value.Immutable
public interface CoverageMetrics extends Serializable {

    @Value.Parameter
    long exceeding();

    @Value.Parameter
    long total();

    static CoverageMetrics of(long exceeding, long total) {
        return com.hartwig.pipeline.adam.ImmutableCoverageMetrics.of(exceeding, total);
    }
}
