package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.JobDefinition;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachineJobDefinition extends JobDefinition {

    String GATK_GERMLINE_IMAGE = "diskimager-gatk-haplotypecaller";

    String imageFamily();

    String startupCommand();

    String completionFlagFile();

    static VirtualMachineJobDefinition germlineCalling(BashStartupScript startupScript) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .imageFamily(GATK_GERMLINE_IMAGE)
                .startupCommand(startupScript.asUnixString())
                .completionFlagFile(startupScript.completionFlag())
                .performanceProfile(PerformanceProfile.singleNode())
                .build();
    }

    static VirtualMachineJobDefinition somaticCalling() {
        return null;
    }
}