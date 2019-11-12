package com.hartwig.pipeline.execution.vm.storage;

import java.util.Collections;
import java.util.List;

public interface StorageStrategy {
    default List<String> initialise() {
        return Collections.emptyList();
    }
}
