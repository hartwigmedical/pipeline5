package com.hartwig.pipeline.alignment;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.execution.dataproc.SparkJobDefinition;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.StatusCheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Job implements SparkExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    private final SparkExecutor decorated;
    private final StatusCheck statusCheck;

    Job(final SparkExecutor decorated, final StatusCheck statusCheck) {
        this.decorated = decorated;
        this.statusCheck = statusCheck;
    }

    public PipelineStatus submit(final RuntimeBucket runtimeBucket, final SparkJobDefinition sparkJobDefinition) {
        try {
            decorated.submit(runtimeBucket, sparkJobDefinition);
            StatusCheck.Status status = statusCheck.check(runtimeBucket);
            if (status == StatusCheck.Status.FAILED) {
                return PipelineStatus.FAILED;
            } else {
                return PipelineStatus.SUCCESS;
            }
        } catch (Exception e) {
            LOGGER.error(String.format("Unable to run job [%s]", sparkJobDefinition), e);
            return PipelineStatus.FAILED;
        }
    }
}
