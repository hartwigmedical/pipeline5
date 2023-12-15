package com.hartwig.pipeline.execution.vm;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class VirtualMachinePerformanceProfileTest {
    @Test
    public void shouldSetUriForCustomProfile() {
        int cores = 16;
        int memoryGb = 32;
        int memoryMb = memoryGb * 1024;
        assertThat(VirtualMachinePerformanceProfile.custom(cores, memoryGb).uri()).isEqualTo(format("custom-%d-%d", cores, memoryMb));
    }
}