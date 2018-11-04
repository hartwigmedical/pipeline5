package com.hartwig.pipeline.performance;

import java.util.Optional;

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

    Optional<Double> fastQSizeGB();

    static ImmutablePerformanceProfile.Builder builder() {
        return ImmutablePerformanceProfile.builder();
    }

    static PerformanceProfile mini() {
        return PerformanceProfile.builder().build();
    }

    static PerformanceProfile singleNode() {
        return PerformanceProfile.builder().numPreemtibleWorkers(0).numPrimaryWorkers(0).master(MachineType.defaultMaster()).build();
    }
}
