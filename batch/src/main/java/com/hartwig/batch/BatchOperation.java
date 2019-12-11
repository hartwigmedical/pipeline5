package com.hartwig.batch;

import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.operations.OperationDescriptor;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.storage.RuntimeBucket;

public interface BatchOperation {
    VirtualMachineJobDefinition execute(InputBundle inputs, RuntimeBucket runtimeBucket, BashStartupScript startupScript,
                                        RuntimeFiles executionFlags);

    OperationDescriptor descriptor();
}
