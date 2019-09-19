package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.execution.PerformanceProfile;

import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachinePerformanceProfile extends PerformanceProfile{

    String uri();

    static VirtualMachinePerformanceProfile defaultVm() {
        return ImmutableVirtualMachinePerformanceProfile.builder().uri(MachineType.defaultVm().uri()).build();
    }

    static VirtualMachinePerformanceProfile custom(int cores, int memoryGb) {
        return ImmutableVirtualMachinePerformanceProfile.builder()
                .uri(format("custom-%d-%d", cores, memoryGb * 1024))
                .build();
    }
}
