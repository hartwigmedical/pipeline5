package com.hartwig.pipeline.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class MachineTypeTest {

    @Test
    public void resolves4StandardWhenCPUsOver0() {
        checkMachineTypeFor(1, MachineType.Google.STANDARD_4);
    }

    @Test
    public void resolves8StandardWhenCPUsOver2() {
        checkMachineTypeFor(3, MachineType.Google.STANDARD_8);
    }

    @Test
    public void resolves16StandardWhenCPUsOver6() {
        checkMachineTypeFor(7, MachineType.Google.STANDARD_16);
    }

    @Test
    public void resolves32StandardWhenCPUsOver14() {
        checkMachineTypeFor(15, MachineType.Google.STANDARD_32);
    }

    @Test
    public void resolves64StandardWhenCPUsOver16() {
        checkMachineTypeFor(31, MachineType.Google.STANDARD_64);
    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentWhenCPUsExceedMax() {
        checkMachineTypeFor(MachineType.MAX_CPUS + 1, MachineType.Google.STANDARD_32);
    }

    private void checkMachineTypeFor(final int cpuPerNode, final MachineType.Google machine) {
        PerformanceProfile testProfile = PerformanceProfile.builder().cpuPerWorker(cpuPerNode).build();
        MachineType victim = MachineType.workerFrom(testProfile);
        assertThat(victim.uri()).isEqualTo(machine.uri());
    }
}