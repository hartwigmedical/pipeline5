package com.hartwig.pipeline.alignment;

import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.execution.dataproc.SparkJobDefinition;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.StatusCheck;
import com.hartwig.pipeline.metrics.Metrics;
import com.hartwig.pipeline.metrics.MetricsTimeline;
import com.hartwig.pipeline.metrics.Monitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;

class Job implements SparkExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    private final SparkExecutor decorated;
    private final CostCalculator costCalculator;
    private final Monitor monitor;
    private final StatusCheck statusCheck;

    Job(final SparkExecutor decorated, final CostCalculator costCalculator, final Monitor monitor, final StatusCheck statusCheck) {
        this.decorated = decorated;
        this.costCalculator = costCalculator;
        this.monitor = monitor;
        this.statusCheck = statusCheck;
    }

    public JobStatus submit(final RuntimeBucket runtimeBucket, final SparkJobDefinition sparkJobDefinition) {
        try {
            MetricsTimeline metricsTimeline = new MetricsTimeline(Clock.systemDefaultZone(), new Metrics(monitor, costCalculator));
            metricsTimeline.start(sparkJobDefinition);
            decorated.submit(runtimeBucket, sparkJobDefinition);
            metricsTimeline.stop(sparkJobDefinition);
            StatusCheck.Status status = statusCheck.check(runtimeBucket);
            if (status == StatusCheck.Status.FAILED) {
                return JobStatus.FAILED;
            } else {
                return JobStatus.SUCCESS;
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Unable to run job [%s]", sparkJobDefinition), e);
            return JobStatus.FAILED;
        }
    }
}
