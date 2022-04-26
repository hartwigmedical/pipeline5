package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.execution.PerformanceProfile;

import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachinePerformanceProfile extends PerformanceProfile {

    @Value.Default
    default String uri() {
        return machineType().uri();
    }

    MachineType machineType();

    static VirtualMachinePerformanceProfile defaultVm() {
        return ImmutableVirtualMachinePerformanceProfile.builder().machineType(MachineType.defaultVm()).build();
    }

    static VirtualMachinePerformanceProfile custom(final int cores, final int memoryGb) {

        return ImmutableVirtualMachinePerformanceProfile.builder().machineType(MachineType.custom(memoryGb, cores)).build();
    }
}
