package com.hartwig.pipeline.execution.vm.storage;

import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalSsdStorageStrategyTest {
    @Test
    public void shouldGenerateInitialisationCommandsForGivenNumberOfRawDevices() {
        int numDevices = 4;
        List<String> lines = new LocalSsdStorageStrategy(numDevices).initialise();
        assertThat(lines).isNotEmpty();
        assertThat(lines.size()).isEqualTo(3);
        assertThat(lines.get(0)).isEqualTo("mdadm --create /dev/md0 --level=0 --raid-devices=4 /dev/nvme0n1 /dev/nvme0n2 /dev/nvme0n3 /dev/nvme0n4");
        assertThat(lines.get(1)).isEqualTo("mkfs.ext4 -F /dev/md0");
        assertThat(lines.get(2)).isEqualTo("mount /dev/md0 /data");
    }
}
