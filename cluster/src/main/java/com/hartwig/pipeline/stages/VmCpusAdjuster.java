package com.hartwig.pipeline.stages;

import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.Arguments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VmCpusAdjuster {
    private static final Logger LOGGER = LoggerFactory.getLogger(VmCpusAdjuster.class);

    private final Arguments arguments;

    public VmCpusAdjuster(Arguments arguments) {
        this.arguments = arguments;
    }

    public VirtualMachineJobDefinition overrideVmDefinition(VirtualMachineJobDefinition jobDefinition) {
        if (arguments.stageCpusOverrideRegex().isEmpty() || arguments.stageCpusOverride().isEmpty()) {
            if (arguments.stageCpusOverrideRegex().isPresent()) {
                LOGGER.warn("Stage cpus override regex is set but no override value is provided (--stage_cpus_override).");
            }
            if (arguments.stageCpusOverride().isPresent()) {
                LOGGER.warn("Stage cpus override value is set but no regex is provided (--stage_cpus_override_regex).");
            }
            return jobDefinition;
        }
        var jobName = jobDefinition.name();
        var regex = arguments.stageCpusOverrideRegex().get();
        if (!jobName.matches(regex)) {
            return jobDefinition;
        }
        var cpus = arguments.stageCpusOverride().get();
        var originalCpus = jobDefinition.performanceProfile().cpus().orElseThrow();
        LOGGER.info("Overriding cpus for job [{}] to {} (original was {})", jobName, cpus, originalCpus);
        var memoryGb = jobDefinition.performanceProfile().memoryGB().orElseThrow();
        return VirtualMachineJobDefinition.builder()
                .from(jobDefinition)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(cpus, memoryGb))
                .build();
    }
}
