package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.dataproc.JarLocation;
import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.execution.dataproc.SparkJobDefinition;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.StatusCheck;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class JobTest {

    private static final JarLocation JAR_LOCATION = JarLocation.of("/path/to/jar");
    private static final SparkJobDefinition JOB_DEFINITION =
            SparkJobDefinition.gunzip(JAR_LOCATION, MockRuntimeBucket.test().getRuntimeBucket());
    private SparkExecutor sparkExecutor;
    private StatusCheck statusCheck;
    private Job victim;
    private RuntimeBucket runtimeBucket;
    private String jobName;

    @Before
    public void setUp() throws Exception {
        sparkExecutor = mock(SparkExecutor.class);
        statusCheck = mock(StatusCheck.class);
        jobName = "job_name";
        victim = new Job(sparkExecutor, statusCheck);
        runtimeBucket = MockRuntimeBucket.of("test_bucket").getRuntimeBucket();
    }

    @Test
    public void reportsJobResultFailedOnException() {
        when(sparkExecutor.submit(runtimeBucket, JOB_DEFINITION)).thenThrow(new RuntimeException());
        assertThat(victim.submit(runtimeBucket, JOB_DEFINITION)).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void reportsJobResultFailedWhenStatusCheckFails() {
        when(statusCheck.check(runtimeBucket, JOB_DEFINITION.name())).thenReturn(StatusCheck.Status.FAILED);
        assertThat(victim.submit(runtimeBucket, JOB_DEFINITION)).isEqualTo(PipelineStatus.FAILED);
    }

    @Test
    public void reportsSuccessWhenStatusCheckReturnsSuccess() {
        when(statusCheck.check(runtimeBucket, JOB_DEFINITION.name())).thenReturn(StatusCheck.Status.SUCCESS);
        assertThat(victim.submit(runtimeBucket, JOB_DEFINITION)).isEqualTo(PipelineStatus.SUCCESS);
    }

    @Test
    public void reportsUnknownWhenStatusCheckReturnsUnknown() {
        when(statusCheck.check(runtimeBucket, JOB_DEFINITION.name())).thenReturn(StatusCheck.Status.UNKNOWN);
        assertThat(victim.submit(runtimeBucket, JOB_DEFINITION)).isEqualTo(PipelineStatus.UNKNOWN);
    }
}