package com.hartwig.pipeline.bootstrap;

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
import com.hartwig.pipeline.cluster.JarLocation;
import com.hartwig.pipeline.cluster.SparkCluster;
import com.hartwig.pipeline.cluster.SparkJobDefinition;
import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.io.StatusCheck;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Stage;
import com.hartwig.pipeline.performance.PerformanceProfile;
import com.hartwig.pipeline.testsupport.MockRuntimeBucket;

import org.junit.Before;
import org.junit.Test;

public class JobTest {

    private static final ImmutableSample TEST_SAMPLE = Sample.builder("", "test_sample").build();
    private static final PerformanceProfile PERFORMANCE_PROFILE = PerformanceProfile.singleNode();
    private static final Arguments ARGUMENTS = Arguments.defaults();
    private static final JarLocation JAR_LOCATION = JarLocation.of("/path/to/jar");
    private static final SparkJobDefinition JOB_DEFINITION = SparkJobDefinition.gunzip(JAR_LOCATION, PERFORMANCE_PROFILE);
    private SparkCluster sparkCluster;
    private CostCalculator costCalculator;
    private Monitor monitor;
    private StatusCheck statusCheck;
    private Job victim;
    private RuntimeBucket runtimeBucket;

    @Before
    public void setUp() throws Exception {
        sparkCluster = mock(SparkCluster.class);
        costCalculator = mock(CostCalculator.class);
        monitor = mock(Monitor.class);
        statusCheck = mock(StatusCheck.class);
        victim = new Job(PERFORMANCE_PROFILE,
                sparkCluster,
                JOB_DEFINITION,
                Stage.gunzip(PERFORMANCE_PROFILE),
                costCalculator,
                monitor, statusCheck);
        runtimeBucket = MockRuntimeBucket.of("test_bucket").getRuntimeBucket();
    }

    @Test
    public void reportsJobResultFailedOnException() throws Exception {
        doThrow(new IOException()).when(sparkCluster).stop(any());
        assertThat(victim.execute(TEST_SAMPLE, runtimeBucket, ARGUMENTS)).isEqualTo(JobResult.FAILED);
    }

    @Test
    public void reportsJobResultFailedWhenStatusCheckFailsTwice() {
        when(statusCheck.check(runtimeBucket)).thenReturn(StatusCheck.Status.FAILED).thenReturn(StatusCheck.Status.FAILED);
        assertThat(victim.execute(TEST_SAMPLE, runtimeBucket, ARGUMENTS)).isEqualTo(JobResult.FAILED);
    }

    @Test
    public void clusterStartedJobSubmittedClusterStopped() throws IOException {
        assertThat(victim.execute(TEST_SAMPLE, runtimeBucket, Arguments.defaults())).isEqualTo(JobResult.SUCCESS);
        verify(sparkCluster).start(PERFORMANCE_PROFILE, TEST_SAMPLE, runtimeBucket, ARGUMENTS);
        verify(sparkCluster).submit(JOB_DEFINITION, ARGUMENTS);
        verify(sparkCluster).stop(ARGUMENTS);
    }

    @Test
    public void metricsSentToMonitorOnJobCompletion() {
        victim.execute(TEST_SAMPLE, runtimeBucket, Arguments.defaults());
        verify(monitor, times(2)).update(any());
    }
}