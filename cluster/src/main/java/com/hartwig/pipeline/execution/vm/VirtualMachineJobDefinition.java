package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.dataproc.DataprocPerformanceProfile;
import com.hartwig.pipeline.execution.JobDefinition;

import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachineJobDefinition extends JobDefinition<VirtualMachinePerformanceProfile> {

    String GATK_GERMLINE_IMAGE = "diskimager-gatk-haplotypecaller";
    String STRELKA_IMAGE = "diskimager-strelka";

    String imageFamily();

    String startupCommand();

    String completionFlagFile();

    static VirtualMachineJobDefinition germlineCalling(BashStartupScript startupScript) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("germline")
                .imageFamily(GATK_GERMLINE_IMAGE)
                .startupCommand(startupScript.asUnixString())
                .completionFlagFile(startupScript.completionFlag())
                .performanceProfile(VirtualMachinePerformanceProfile.defaultVm())
                .build();
    }

    static VirtualMachineJobDefinition somaticCalling(BashStartupScript startupScript) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("strelka")
                .imageFamily(STRELKA_IMAGE)
                .startupCommand(startupScript.asUnixString())
                .completionFlagFile(startupScript.completionFlag())
                .performanceProfile(VirtualMachinePerformanceProfile.defaultVm())
                .build();
    }
}