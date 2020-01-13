package com.hartwig.pipeline.execution.vm.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

public class PersistentStorageStrategyTest {

    @Test
    public void mountsSecondVolumeDeviceToData() {
        PersistentStorageStrategy victim = new PersistentStorageStrategy();
        List<String> results = victim.initialise();
        assertThat(results).containsExactly("mkdir -p /data", "mkfs.ext4 -F /dev/sdb", "mount /dev/sdb /data");
    }
}