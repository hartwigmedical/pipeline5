package com.hartwig.pipeline.cluster;

import java.io.IOException;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.performance.PerformanceProfile;

public interface SampleCluster {

    void start(PerformanceProfile performanceProfile, Sample sample, RuntimeBucket runtimeBucket, Arguments arguments) throws IOException;

    void submit(PerformanceProfile performanceProfile, SparkJobDefinition jobDefinition, Arguments arguments) throws IOException;

    void stop(Arguments arguments) throws IOException;
}
