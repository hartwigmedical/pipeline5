package com.hartwig.pipeline.adam;

import java.io.Serializable;

import org.immutables.value.Value;

@Value.Immutable
public interface CoverageThreshold extends Serializable {

    @Value.Parameter
    double coverage();

    @Value.Parameter
    double minimumPercentage();

    static CoverageThreshold of(double coverage, double minimumPercentage) {
        return ImmutableCoverageThreshold.of(coverage, minimumPercentage);
    }
}
