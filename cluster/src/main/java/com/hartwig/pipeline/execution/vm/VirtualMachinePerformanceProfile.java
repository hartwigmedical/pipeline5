package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.execution.PerformanceProfile;
import org.immutables.value.Value;

import static java.lang.String.format;

@Value.Immutable
public interface VirtualMachinePerformanceProfile extends PerformanceProfile{

    String uri();

    @Value.Default
    default int diskGb() {
        return 1000;
    }

    static VirtualMachinePerformanceProfile defaultVm() {
        return ImmutableVirtualMachinePerformanceProfile.builder().uri(MachineType.defaultWorker().uri()).build();
    }

    static VirtualMachinePerformanceProfile highCpuVm() {
        return ImmutableVirtualMachinePerformanceProfile.builder().uri(MachineType.highCpu().uri()).build();
    }

    static VirtualMachinePerformanceProfile custom(int cores, int memoryGb) {
        return ImmutableVirtualMachinePerformanceProfile.builder()
                .uri(format("custom-%d-%d", cores, memoryGb * 1024))
                .build();
    }
}
