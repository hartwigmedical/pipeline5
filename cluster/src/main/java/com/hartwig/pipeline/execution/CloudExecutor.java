package com.hartwig.pipeline.execution;

import com.hartwig.pipeline.storage.RuntimeBucket;

public interface CloudExecutor<T extends JobDefinition> {

    PipelineStatus submit(RuntimeBucket bucket, T jobDefinition);
}
