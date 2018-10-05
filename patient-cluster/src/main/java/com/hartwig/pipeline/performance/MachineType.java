package com.hartwig.pipeline.performance;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

@Value.Immutable
public interface MachineType {

    String GOOGLE_STANDARD_16 = "n1-standard-16";
    String GOOGLE_STANDARD_32 = "n1-standard-32";
    String GOOGLE_HIGHMEM_32 = "n1-standard-32";
    int DISK_GB = 1000;

    String uri();

    int memoryGB();

    int cpus();

    int diskGB();

    double costPerInstancePerHour();

    static MachineType defaultWorker() {
        return highMemoryWorker();
    }

    @NotNull
    static MachineType standardWorker() {
        return ImmutableMachineType.builder().uri(GOOGLE_STANDARD_32).memoryGB(120).cpus(32).diskGB(DISK_GB).costPerInstancePerHour(1.52).build();
    }

    static MachineType highMemoryWorker() {
        return ImmutableMachineType.builder()
                .uri(GOOGLE_HIGHMEM_32)
                .memoryGB(120)
                .cpus(32)
                .diskGB(DISK_GB)
                .costPerInstancePerHour(1.52)
                .build();
    }

    static MachineType defaultPreemtibleWorker() {
        return ImmutableMachineType.builder().from(defaultWorker()).costPerInstancePerHour(0.32).build();
    }

    static MachineType defaultMaster() {
        return ImmutableMachineType.builder()
                .uri(GOOGLE_STANDARD_16)
                .memoryGB(60)
                .cpus(16)
                .diskGB(1000)
                .costPerInstancePerHour(0.76)
                .build();
    }

    static ImmutableMachineType.Builder builder() {
        return ImmutableMachineType.builder();
    }
}
