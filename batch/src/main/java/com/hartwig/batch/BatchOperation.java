package com.hartwig.batch;

import com.hartwig.batch.operations.CommandDescriptor;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.storage.RuntimeBucket;

public interface BatchOperation {
    VirtualMachineJobDefinition execute(String input, RuntimeBucket bucket, String instanceId);

    CommandDescriptor descriptor();
}
