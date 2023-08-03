package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.JobDefinition;
import com.hartwig.pipeline.tools.VersionUtils;

import org.immutables.value.Value;

@Value.Immutable
public interface VirtualMachineJobDefinition extends JobDefinition<VirtualMachinePerformanceProfile> {

    String STANDARD_IMAGE = "pipeline5-" + VersionUtils.imageVersion();
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
}