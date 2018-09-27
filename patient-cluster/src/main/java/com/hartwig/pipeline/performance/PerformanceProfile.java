package com.hartwig.pipeline.performance;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutablePerformanceProfile.class)
public interface PerformanceProfile {

    int numPrimaryWorkers();

    int numPreemtibleWorkers();

    @Value.Default
    default MachineType master() {
        return MachineType.defaultMaster();
    }

    @Value.Default
    default MachineType primaryWorkers() {
        return MachineType.defaultWorker();
    }

    @Value.Default
    default MachineType preemtibleWorkers() {
        return MachineType.defaultPreemtibleWorker();
    }

    static ImmutablePerformanceProfile.Builder builder() {
        return ImmutablePerformanceProfile.builder();
    }
}
