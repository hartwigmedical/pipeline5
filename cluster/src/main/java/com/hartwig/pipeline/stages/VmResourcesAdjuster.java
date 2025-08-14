package com.hartwig.pipeline.stages;

import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.Arguments;

public class VmResourcesAdjuster {

    private final VmCpusAdjuster cpusAdjuster;
    private final VmMemoryAdjuster memoryAdjuster;

    public VmResourcesAdjuster(Arguments arguments) {
        this.cpusAdjuster = new VmCpusAdjuster(arguments);
        this.memoryAdjuster = new VmMemoryAdjuster(arguments);
    }

    public VirtualMachineJobDefinition overrideVmDefinition(VirtualMachineJobDefinition jobDefinition) {
        return memoryAdjuster.overrideVmDefinition(cpusAdjuster.overrideVmDefinition(jobDefinition));
    }
}
