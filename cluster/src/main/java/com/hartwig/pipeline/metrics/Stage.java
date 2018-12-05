package com.hartwig.pipeline.metrics;

import com.hartwig.pipeline.performance.PerformanceProfile;

import org.immutables.value.Value;

@Value.Immutable
public interface Stage {

    @Value.Parameter
    String name();

    @Value.Parameter
    PerformanceProfile performanceProfile();

    static Stage bam(PerformanceProfile profile) {
        return ImmutableStage.of("BAM", profile);
    }

    static Stage sortAndIndex(PerformanceProfile profile) {
        return ImmutableStage.of("SORT_INDEX", profile);
    }

    static Stage gunzip(PerformanceProfile profile) {
        return ImmutableStage.of("GUNZIP", profile);
    }
}
