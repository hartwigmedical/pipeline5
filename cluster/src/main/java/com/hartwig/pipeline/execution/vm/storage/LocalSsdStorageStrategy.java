package com.hartwig.pipeline.execution.vm.storage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;

public class LocalSsdStorageStrategy implements StorageStrategy {
    private static final String MD_DEV = "/dev/md0";
    private final int numberOfSsdDevices;

    public LocalSsdStorageStrategy(final int numberOfSsdDevices) {
        this.numberOfSsdDevices = numberOfSsdDevices;
    }

    @Override
    public List<String> initialise() {
        String raidDevice = "/dev/md0";
        String mdadm = format("mdadm --create %s --level=0 --raid-devices=%d ", raidDevice, numberOfSsdDevices);
            for (int i = 1; i <= numberOfSsdDevices; i++) {
                mdadm += format("/dev/nvme0n%d ", i);
            }
        return asList("mkdir -p /data", mdadm.trim(), "mkfs.ext4 -F " + MD_DEV, format("mount %s /data", MD_DEV));
    }
}
