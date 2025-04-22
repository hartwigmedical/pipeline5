package com.hartwig.pipeline.stages;

import java.util.OptionalInt;

import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.Arguments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VmMemoryAdjuster {
    private static final Logger LOGGER = LoggerFactory.getLogger(VmMemoryAdjuster.class);

    private final Arguments arguments;

    public VmMemoryAdjuster(Arguments arguments) {
        this.arguments = arguments;
    }

    public VirtualMachineJobDefinition overrideVmDefinition(VirtualMachineJobDefinition jobDefinition) {
        if (arguments.stageMemoryOverrideRegex().isEmpty() || arguments.stageMemoryOverrideGb().isEmpty()) {
            if (arguments.stageMemoryOverrideRegex().isPresent()) {
                LOGGER.warn("Stage memory override regex is set but no override value is provided (--stage_memory_override_gb).");
            }
            if (arguments.stageMemoryOverrideGb().isPresent()) {
                LOGGER.warn("Stage memory override value is set but no regex is provided (--stage_memory_override_regex).");
            }
            return jobDefinition;
        }
        var jobName = jobDefinition.name();
        var regex = arguments.stageMemoryOverrideRegex().get();
        if (!jobName.matches(regex)) {
            return jobDefinition;
        }
        var memoryGb = arguments.stageMemoryOverrideGb().get();
        var originalMemoryGb = jobDefinition.performanceProfile().memoryGB().orElseThrow();
        LOGGER.info("Overriding memory for job [{}] to {}GB (original was {}GB)", jobName, memoryGb, originalMemoryGb);
        var cpus = jobDefinition.performanceProfile().cpus().orElseThrow();
        return VirtualMachineJobDefinition.builder()
                .from(jobDefinition)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(cpus, memoryGb))
                .build();
    }
}
