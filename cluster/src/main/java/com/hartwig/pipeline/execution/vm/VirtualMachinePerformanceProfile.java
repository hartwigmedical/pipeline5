package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.MachineType;
import com.hartwig.pipeline.execution.PerformanceProfile;
import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachinePerformanceProfile extends PerformanceProfile{

    MachineType virtualMachineType();

    static VirtualMachinePerformanceProfile defaultVm() {
        return ImmutableVirtualMachinePerformanceProfile.builder().virtualMachineType(MachineType.defaultWorker()).build();
    }

    static VirtualMachinePerformanceProfile highCpuVm() {
        return ImmutableVirtualMachinePerformanceProfile.builder().virtualMachineType(MachineType.highCpu()).build();
    }
}
