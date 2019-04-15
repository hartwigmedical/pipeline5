package com.hartwig.pipeline.alignment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.hartwig.patient.ImmutableSample;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.dataproc.JarLocation;
import com.hartwig.pipeline.execution.dataproc.SparkExecutor;
import com.hartwig.pipeline.execution.dataproc.SparkJobDefinition;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.StatusCheck;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.performance.PerformanceProfile;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class JobTest {

    private static final ImmutableSample TEST_SAMPLE = Sample.builder("", "test_sample").build();
    private static final PerformanceProfile PERFORMANCE_PROFILE = PerformanceProfile.singleNode();
    private static final Arguments ARGUMENTS = Arguments.testDefaults();
    private static final JarLocation JAR_LOCATION = JarLocation.of("/path/to/jar");
    private static final SparkJobDefinition JOB_DEFINITION = SparkJobDefinition.gunzip(JAR_LOCATION);
    private SparkExecutor sparkExecutor;
    private Monitor monitor;
    private StatusCheck statusCheck;
    private Job victim;
    private RuntimeBucket runtimeBucket;

    @Before
    public void setUp() throws Exception {
        sparkExecutor = mock(SparkExecutor.class);
        final CostCalculator costCalculator = mock(CostCalculator.class);
        monitor = mock(Monitor.class);
        statusCheck = mock(StatusCheck.class);
        victim = new Job(sparkExecutor, costCalculator, monitor, statusCheck);
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

    @Test
    public void metricsSentToMonitorOnJobCompletion() {
        victim.submit(runtimeBucket, JOB_DEFINITION);
        verify(monitor, times(2)).update(any());
    }
}