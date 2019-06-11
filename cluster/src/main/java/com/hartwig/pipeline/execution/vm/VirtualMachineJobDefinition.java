package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.JobDefinition;
import com.hartwig.pipeline.io.ResultsDirectory;
import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachineJobDefinition extends JobDefinition<VirtualMachinePerformanceProfile> {

    String STANDARD_IMAGE = "diskimager-standard";

    @Value.Default
    default String imageFamily() {
        return STANDARD_IMAGE;
    }

    BashStartupScript startupCommand();

    ResultsDirectory namespacedResults();

    @Override
    @Value.Default
    default VirtualMachinePerformanceProfile performanceProfile() {
        return VirtualMachinePerformanceProfile.defaultVm();
    }

    static ImmutableVirtualMachineJobDefinition.Builder builder() {
        return ImmutableVirtualMachineJobDefinition.builder();
    }

    static VirtualMachineJobDefinition snpGenotyping(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("snpgenotype")
                .startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 16))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition germlineCalling(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("germline")
                .startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(16, 32))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition somaticCalling(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("strelka")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition structuralCalling(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("gridss")
                .startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 32))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition amber(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("amber")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition cobalt(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("cobalt")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(16, 16))
                .build();
    }

    static VirtualMachineJobDefinition purple(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("purple")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(4, 8))
                .build();
    }

    static VirtualMachineJobDefinition bamMetrics(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("bam-metrics")
                .startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 32))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition healthChecker(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("health-checker")
                .startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(8, 32))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition flagstat(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("flagstat")
                .startupCommand(startupScript)
                .performanceProfile(VirtualMachinePerformanceProfile.defaultVm())
                .namespacedResults(resultsDirectory)
                .build();
    }
}