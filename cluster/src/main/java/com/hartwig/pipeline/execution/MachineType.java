package com.hartwig.pipeline.execution;

import org.immutables.value.Value;

@Value.Immutable
public interface MachineType {

    String GOOGLE_STANDARD_16 = "n1-standard-16";
    String GOOGLE_STANDARD_32 = "n1-standard-32";
    String GOOGLE_HIGHMEM_32 = "n1-highmem-32";

    String uri();

    int memoryGB();

    int cpus();

    static MachineType defaultVm() {
        return ImmutableMachineType.builder().uri(GOOGLE_STANDARD_32).memoryGB(120).cpus(32).build();
    }

    static MachineType defaultWorker() {
        return highMemoryWorker();
    }

    static MachineType highMemoryWorker() {
        return ImmutableMachineType.builder().uri(GOOGLE_HIGHMEM_32).memoryGB(208).cpus(32).build();
    }

    static MachineType defaultPreemtibleWorker() {
        return ImmutableMachineType.builder().from(defaultWorker()).build();
    }

    static MachineType defaultMaster() {
        return ImmutableMachineType.builder().uri(GOOGLE_STANDARD_16).memoryGB(60).cpus(16).build();
    }

    static ImmutableMachineType.Builder builder() {
        return ImmutableMachineType.builder();
    }
}
