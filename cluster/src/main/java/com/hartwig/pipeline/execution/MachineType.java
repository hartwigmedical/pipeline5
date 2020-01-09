package com.hartwig.pipeline.execution;

import static java.lang.String.format;

import org.immutables.value.Value;

@Value.Immutable
public interface MachineType {

    String GOOGLE_STANDARD_16 = "n1-standard-16";
    String GOOGLE_STANDARD_32 = "n1-standard-32";
    String GOOGLE_HIGHMEM_32 = "n1-highmem-32";
    int DEFAULT_DISK_SIZE = 500;

    String uri();

    int memoryGB();

    int cpus();

    @Value.Default
    default int diskSizeGB() {
        return DEFAULT_DISK_SIZE;
    }

    static MachineType defaultVm() {
        return ImmutableMachineType.builder().uri(GOOGLE_STANDARD_32).memoryGB(120).cpus(32).build();
    }

    static MachineType defaultWorker() {
        return highMemoryWorker();
    }

    static MachineType highMemoryWorker() {
        return ImmutableMachineType.builder().uri(GOOGLE_HIGHMEM_32).memoryGB(208).cpus(32).diskSizeGB(250).build();
    }

    static MachineType defaultPreemtibleWorker() {
        return ImmutableMachineType.builder().from(defaultWorker()).build();
    }

    static MachineType defaultMaster() {
        return ImmutableMachineType.builder().uri(GOOGLE_STANDARD_16).memoryGB(60).cpus(16).diskSizeGB(1000).build();
    }

    static MachineType custom(int memoryGB, int cores) {
        return ImmutableMachineType.builder().uri(format("custom-%d-%d", cores, memoryGB * 1024)).cpus(cores).memoryGB(memoryGB).build();
    }

    static ImmutableMachineType.Builder builder() {
        return ImmutableMachineType.builder();
    }
}
