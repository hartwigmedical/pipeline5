package com.hartwig.pipeline.performance;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutablePerformanceProfile.class)
public interface PerformanceProfile {

    @Value.Default
    default int numPrimaryWorkers() {
        return 2;
    }

    @Value.Default
    default int numPreemtibleWorkers() {
        return 0;
    }

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
