package com.hartwig.pipeline.performance;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;

@Value.Immutable
public interface MachineType {

    String GOOGLE_STANDARD_16 = "n1-standard-16";
    String GOOGLE_STANDARD_32 = "n1-standard-32";
    String GOOGLE_STANDARD_64 = "n1-standard-64";
    String GOOGLE_HIGHMEM_32 = "n1-highmem-32";
    int DISK_GB = 1000;

    String uri();

    int memoryGB();

    int cpus();

    int diskGB();

    static MachineType beefy() {
        return ImmutableMachineType.builder().uri(GOOGLE_STANDARD_64).memoryGB(240).cpus(64).diskGB(DISK_GB).build();
    }

    static MachineType defaultWorker() {
        return highMemoryWorker();
    }

    @NotNull
    static MachineType standardWorker() {
        return ImmutableMachineType.builder().uri(GOOGLE_STANDARD_32).memoryGB(120).cpus(32).diskGB(DISK_GB).build();
    }

    static MachineType highMemoryWorker() {
        return ImmutableMachineType.builder().uri(GOOGLE_HIGHMEM_32).memoryGB(208).cpus(32).diskGB(DISK_GB).build();
    }

    static MachineType mini() {
        return ImmutableMachineType.builder().uri("n1-standard-2").memoryGB(7).cpus(2).diskGB(DISK_GB).build();
    }

    static MachineType defaultPreemtibleWorker() {
        return ImmutableMachineType.builder().from(defaultWorker()).build();
    }

    static MachineType defaultMaster() {
        return ImmutableMachineType.builder().uri(GOOGLE_STANDARD_16).memoryGB(60).cpus(16).diskGB(1000).build();
    }

    static ImmutableMachineType.Builder builder() {
        return ImmutableMachineType.builder();
    }
}
