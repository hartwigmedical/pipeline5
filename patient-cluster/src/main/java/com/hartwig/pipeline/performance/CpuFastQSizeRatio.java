package com.hartwig.pipeline.performance;

import org.immutables.value.Value;

@Value.Immutable
public interface CpuFastQSizeRatio {

    @Value.Parameter
    double cpusPerGB();

    static CpuFastQSizeRatio of(double ratio) {
        return ImmutableCpuFastQSizeRatio.of(ratio);
    }
}
