package com.hartwig.pipeline.bootstrap;

import java.io.IOException;
import java.time.Clock;

import com.hartwig.patient.Sample;
import com.hartwig.pipeline.cluster.SparkCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.StatusCheck;
import com.hartwig.pipeline.metrics.Metrics;
import com.hartwig.pipeline.metrics.MetricsTimeline;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Stage;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Job {

    private final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    private final PerformanceProfile performanceProfile;
    private final SparkCluster targetCluster;
    private final SparkJobDefinition jobDefinition;
    private final Stage stage;
    private final CostCalculator costCalculator;
    private final Monitor monitor;
    private final StatusCheck statusCheck;

    Job(final PerformanceProfile performanceProfile, final SparkCluster targetCluster, final SparkJobDefinition jobDefinition,
            final Stage stage, final CostCalculator costCalculator, final Monitor monitor, final StatusCheck statusCheck) {
        this.performanceProfile = performanceProfile;
        this.targetCluster = targetCluster;
        this.jobDefinition = jobDefinition;
        this.stage = stage;
        this.costCalculator = costCalculator;
        this.monitor = monitor;
        this.statusCheck = statusCheck;
    }

    String getName() {
        return jobDefinition.name();
    }

    JobResult execute(Sample sample, RuntimeBucket runtimeBucket, Arguments arguments) {
        try {
            MetricsTimeline metricsTimeline = new MetricsTimeline(Clock.systemDefaultZone(), new Metrics(monitor, costCalculator));
            metricsTimeline.start(stage);
            targetCluster.start(performanceProfile, sample, runtimeBucket, arguments);
            targetCluster.submit(jobDefinition, arguments);
            stopCluster(arguments, targetCluster);
            metricsTimeline.stop(stage);
            StatusCheck.Status status = statusCheck.check(runtimeBucket);
            if (status == StatusCheck.Status.FAILED) {
                return JobResult.FAILED;
            } else {
                return JobResult.SUCCESS;
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Unable to run job [%s]", jobDefinition), e);
            return JobResult.FAILED;
        }
    }

    private void stopCluster(final Arguments arguments, final SparkCluster cluster) throws IOException {
        cluster.stop(arguments);
    }
}
