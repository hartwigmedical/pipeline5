package com.hartwig.pipeline.execution.dataproc;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.execution.PerformanceProfile;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableDataprocPerformanceProfile.class)
public interface DataprocPerformanceProfile extends PerformanceProfile{

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

    static ImmutableDataprocPerformanceProfile.Builder builder() {
        return ImmutableDataprocPerformanceProfile.builder();
    }

    static DataprocPerformanceProfile mini() {
        return DataprocPerformanceProfile.builder().build();
    }

    static DataprocPerformanceProfile singleNode() {
        return DataprocPerformanceProfile.builder().numPreemtibleWorkers(0).numPrimaryWorkers(0).master(MachineType.defaultMaster()).build();
    }
}
