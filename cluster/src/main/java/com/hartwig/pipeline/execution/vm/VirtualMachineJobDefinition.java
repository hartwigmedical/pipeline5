package com.hartwig.pipeline.execution.vm;

import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.JobDefinition;
import com.hartwig.pipeline.tools.Versions;

import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachineJobDefinition extends JobDefinition<VirtualMachinePerformanceProfile> {

    String STANDARD_IMAGE = "pipeline5-" + Versions.imageVersion();
    String HMF_IMAGE_PROJECT = "hmf-images";
    String PUBLIC_IMAGE_NAME = "hmf-public-pipeline-v1";

    @Value.Derived
    default long baseImageDiskSizeGb() {
        return 200L;
    }

    @Value.Default
    default String imageFamily() {
        return STANDARD_IMAGE;
    }

    @Value.Default
    default long workingDiskSpaceGb() {
        return 1200L;
    }

    int LOCAL_SSD_DISK_SPACE_GB = 375;

    BashStartupScript startupCommand();

    ResultsDirectory namespacedResults();

    @Value.Derived
    default long totalPersistentDiskSizeGb() {
        return baseImageDiskSizeGb() + workingDiskSpaceGb();
    }

    @Value.Derived
    default int localSsdCount() {
        int localSsdDeviceSizeGb = LOCAL_SSD_DISK_SPACE_GB;

        int floor = Math.toIntExact(workingDiskSpaceGb() / localSsdDeviceSizeGb);
        long remainder = workingDiskSpaceGb() % localSsdDeviceSizeGb;
        if (remainder != 0) {
            floor++;
        }
        return floor;
    }

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
                .performanceProfile(custom(4, 16))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition germlineCalling(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("germline")
                .startupCommand(startupScript)
                .performanceProfile(custom(32, 40))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition sageSomaticCalling(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("sage-somatic")
                .startupCommand(startupScript)
                .performanceProfile(custom(32, 120))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition sageGermlineCalling(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("sage-germline")
                .performanceProfile(custom(4, 16))
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition structuralCalling(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("gridss")
                .startupCommand(startupScript)
                .performanceProfile(custom(12, 64))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition gripss(String name, BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name(name)
                .startupCommand(startupScript)
                .performanceProfile(custom(4, 24))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition purple(BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("purple")
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(4, 16))
                .workingDiskSpaceGb(LOCAL_SSD_DISK_SPACE_GB)
                .build();
    }

    static VirtualMachineJobDefinition pave(String name, BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name(name)
                .startupCommand(bash)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(4, 24))
                .workingDiskSpaceGb(LOCAL_SSD_DISK_SPACE_GB)
                .build();
    }

    static VirtualMachineJobDefinition amber(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("amber")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(32, 120))
                .build();
    }

    static VirtualMachineJobDefinition virusbreakend(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("virusbreakend")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(12, 64))
                .build();
    }

    static VirtualMachineJobDefinition cobalt(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("cobalt")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(16, 16))
                .build();
    }

    static VirtualMachineJobDefinition bamMetrics(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("bam-metrics")
                .startupCommand(startupScript)
                .performanceProfile(custom(8, 32))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition healthChecker(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("health-checker")
                .startupCommand(startupScript)
                .performanceProfile(custom(8, 32))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition flagstat(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("flagstat")
                .startupCommand(startupScript)
                .performanceProfile(custom(32, 120))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition alignment(String lane, BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("aligner-" + lane)
                .startupCommand(startupScript)
                .performanceProfile(custom(96, 96))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition mergeMarkdups(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("merge-markdup")
                .startupCommand(startupScript)
                .performanceProfile(custom(32, 120))
                .namespacedResults(resultsDirectory)
                .build();
    }

    static VirtualMachineJobDefinition linx(String type, BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("linx-" + type)
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(4, 12))
                .workingDiskSpaceGb(LOCAL_SSD_DISK_SPACE_GB)
                .build();
    }

    static VirtualMachineJobDefinition chord(BashStartupScript startupScript, ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("chord")
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(4, 12))
                .workingDiskSpaceGb(LOCAL_SSD_DISK_SPACE_GB)
                .build();
    }
}