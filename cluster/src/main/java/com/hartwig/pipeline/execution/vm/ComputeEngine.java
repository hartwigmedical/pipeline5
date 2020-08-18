package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.RuntimeBucket;

public interface ComputeEngine {

    PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition);

    PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition, String discriminator);
}
