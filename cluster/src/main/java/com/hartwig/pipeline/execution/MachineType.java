package com.hartwig.pipeline.execution;

import static java.lang.String.format;

import org.immutables.value.Value;

@Value.Immutable
public interface MachineType {

    int DEFAULT_DISK_SIZE = 500;

    String uri();

    int memoryGB();

    int cpus();

    @Value.Default
    default int diskSizeGB() {
        return DEFAULT_DISK_SIZE;
    }

    static MachineType defaultVm() {
        return custom(8, 8);
    }

    static MachineType custom(final int memoryGB, final int cores) {
        return ImmutableMachineType.builder().uri(format("custom-%d-%d", cores, memoryGB * 1024)).cpus(cores).memoryGB(memoryGB).build();
    }

    static ImmutableMachineType.Builder builder() {
        return ImmutableMachineType.builder();
    }
}
