package com.hartwig.pipeline.cluster;

import org.immutables.value.Value;

@Value.Immutable
public interface PerformanceProfile {

    @Value.Default
    default int workers() {
        return 7;
    }

    @Value.Default
    default int cpuPerNode() {
        return 16;
    }

    @Value.Default
    default int heapPerNodeGB() {
        return 48;
    }

    @Value.Default
    default int offHeapPerNodeGB() {
        return 48;
    }

    @Value.Default
    default int diskSizeGB() {
        return 1000;
    }

    static ImmutablePerformanceProfile.Builder builder() {
        return ImmutablePerformanceProfile.builder();
    }

    static PerformanceProfile defaultProfile() {
        return builder().build();
    }
}
