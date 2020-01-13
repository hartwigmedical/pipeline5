package com.hartwig.pipeline.execution.vm.storage;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

public class PersistentStorageStrategy implements StorageStrategy {
    @Override
    public List<String> initialise() {
        String device = "/dev/whatever";
        String mountPoint = "/data";
        return asList("mkdir -p " + mountPoint, "mkfs.ext4 -F " + device, format("mount %s %s", device, mountPoint));
    }
}
