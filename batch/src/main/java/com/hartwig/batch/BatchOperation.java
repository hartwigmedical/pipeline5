package com.hartwig.batch;

import com.hartwig.batch.operations.CommandDescriptor;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.storage.RuntimeBucket;

public interface BatchOperation {
    VirtualMachineJobDefinition execute(InputFileDescriptor descriptor, RuntimeBucket runtimeBucket, BashStartupScript startupScript,
            RuntimeFiles executionFlags);

    CommandDescriptor descriptor();
}
