package com.hartwig.pipeline.execution.vm.storage;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;

public class PersistentStorageStrategy implements StorageStrategy {
    @Override
    public List<String> initialise() {
        String device = "/dev/sdb";
        String mountPoint = "/data";
        return asList("mkdir -p " + mountPoint, "mkfs.ext4 -F " + device, format("mount %s %s", device, mountPoint));
    }
}