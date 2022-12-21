package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpComputeEngine implements ComputeEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpComputeEngine.class);

    @Override
    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition) {
        LOGGER.info("Skipping compute on event-only pipeline invocation");
        return PipelineStatus.SUCCESS;
    }

    @Override
    public PipelineStatus submit(final RuntimeBucket bucket, final VirtualMachineJobDefinition jobDefinition, final String discriminator) {
        LOGGER.info("Skipping compute on event-only pipeline invocation");
        return PipelineStatus.SUCCESS;
    }
}
