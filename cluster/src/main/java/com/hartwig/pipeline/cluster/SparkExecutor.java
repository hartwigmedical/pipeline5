package com.hartwig.pipeline.cluster;

import com.hartwig.pipeline.alignment.JobResult;
import com.hartwig.pipeline.io.RuntimeBucket;

public interface SparkExecutor {

    JobResult submit(RuntimeBucket runtimeBucket, SparkJobDefinition jobDefinition);
}
