package com.hartwig.pipeline.execution.vm.storage;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class LocalSsdStorageStrategy implements StorageStrategy {
    private static final String MD_DEV = "/dev/md0";
    private final int numberOfSsdDevices;

    public LocalSsdStorageStrategy(int numberOfSsdDevices) {
        this.numberOfSsdDevices = numberOfSsdDevices;
    }

    @Override
    public List<String> initialise() {
        String raidDevice = "/dev/md0";
        StringBuilder mdadm = new StringBuilder(format("mdadm --create %s --level=0 --raid-devices=%d ", raidDevice, numberOfSsdDevices));
            for (int i = 1; i <= numberOfSsdDevices; i++) {
                mdadm.append(format("/dev/nvme0n%d ", i));
            }
        return asList(mdadm.toString().trim(), "mkfs.ext4 -F " + MD_DEV, format("mount %s /data", MD_DEV));
    }
}
