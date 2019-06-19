package com.hartwig.pipeline.execution.dataproc;

import com.hartwig.pipeline.execution.CloudExecutor;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.io.RuntimeBucket;

public interface SparkExecutor extends CloudExecutor<SparkJobDefinition> {

    PipelineStatus submit(RuntimeBucket runtimeBucket, SparkJobDefinition jobDefinition);
}
