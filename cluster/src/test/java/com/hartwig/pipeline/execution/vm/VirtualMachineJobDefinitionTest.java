package com.hartwig.pipeline.execution.vm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.hartwig.pipeline.ResultsDirectory;

import org.junit.Before;
import org.junit.Test;

public class VirtualMachineJobDefinitionTest {
    private static final long BASE_IMAGE_SIZE_GB = 200L;
    private static final int LOCAL_SSD_DEVICE_CAPACITY_GB = 375;

    private VirtualMachineJobDefinition victim;
    private ImmutableVirtualMachineJobDefinition.Builder builder;

    @Before
    public void setup() {
        BashStartupScript startupScript = mock(BashStartupScript.class);
        builder = VirtualMachineJobDefinition.builder().startupCommand(startupScript)
                .namespacedResults(ResultsDirectory.defaultDirectory())
                .name("victim");
    }

    @Test
    public void shouldDeriveTotalSizeForPersistentDisk() {
        long workingSpace = 900L;
        victim = builder.workingDiskSpaceGb(workingSpace).build();
        assertThat(victim.totalPersistentDiskSizeGb()).isEqualTo(workingSpace + BASE_IMAGE_SIZE_GB);
    }

    @Test
    public void shouldDeriveLocalSsdCountFromWorkingDiskSpace() {
        int ssdCount = 4;
        victim = builder.workingDiskSpaceGb(ssdCount * LOCAL_SSD_DEVICE_CAPACITY_GB - 100).build();
        assertThat(victim.localSsdCount()).isEqualTo(ssdCount);
    }

    @Test
    public void shouldReturnNextHigherEvenNumberIfCalculatedSsdRequirementIsOddToAvoidRuntimeErrorsFromApi() {
        victim = builder.workingDiskSpaceGb(15 * LOCAL_SSD_DEVICE_CAPACITY_GB - 100).build();
        assertThat(victim.localSsdCount()).isEqualTo(16);
    }

    @Test
    public void shouldReturnExactNumberOfSsdsIfRequiredCapacityIsExactEvenMultipleOfDeviceSize() {
        victim = builder.workingDiskSpaceGb(6 * LOCAL_SSD_DEVICE_CAPACITY_GB).build();
        assertThat(victim.localSsdCount()).isEqualTo(6);
    }
}