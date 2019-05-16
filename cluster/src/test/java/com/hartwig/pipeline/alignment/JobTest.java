package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.dataproc.JarLocation;
import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.execution.dataproc.SparkJobDefinition;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.StatusCheck;
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

    @Before
    public void setUp() throws Exception {
        sparkExecutor = mock(SparkExecutor.class);
        statusCheck = mock(StatusCheck.class);
        victim = new Job(sparkExecutor, statusCheck);
        runtimeBucket = MockRuntimeBucket.of("test_bucket").getRuntimeBucket();
    }

    @Test
    public void reportsJobResultFailedOnException() throws Exception {
        when(sparkExecutor.submit(runtimeBucket, JOB_DEFINITION)).thenThrow(new RuntimeException());
        assertThat(victim.submit(runtimeBucket, JOB_DEFINITION)).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void reportsJobResultFailedWhenStatusCheckFails() {
        when(statusCheck.check(runtimeBucket)).thenReturn(StatusCheck.Status.FAILED);
        assertThat(victim.submit(runtimeBucket, JOB_DEFINITION)).isEqualTo(JobStatus.FAILED);
    }

    @Test
    public void reportsSuccessWhenDecoratedExecutorSuccessful() throws IOException {
        when(sparkExecutor.submit(runtimeBucket, JOB_DEFINITION)).thenReturn(JobStatus.SUCCESS);
        assertThat(victim.submit(runtimeBucket, JOB_DEFINITION)).isEqualTo(JobStatus.SUCCESS);
    }
}