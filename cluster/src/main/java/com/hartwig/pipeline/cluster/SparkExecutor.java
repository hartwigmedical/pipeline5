package com.hartwig.pipeline.cluster;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.bootstrap.JobResult;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.performance.PerformanceProfile;

public interface SparkExecutor {

    JobResult submit(RuntimeBucket runtimeBucket, SparkJobDefinition jobDefinition);
}
