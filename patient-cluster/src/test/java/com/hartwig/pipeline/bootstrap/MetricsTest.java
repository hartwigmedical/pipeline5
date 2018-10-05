package com.hartwig.pipeline.bootstrap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.pipeline.cost.CostCalculator;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MetricsTest {

    private static final double COST = 100.0;
    private static final int ONE_HOUR = 3600000;
    private ArgumentCaptor<Metric> metricArgumentCaptor;

    @Before
    public void setUp() throws Exception {
        metricArgumentCaptor = ArgumentCaptor.forClass(Metric.class);
        final Monitor monitor = mock(Monitor.class);
        final CostCalculator costCalculator = mock(CostCalculator.class);
        final PerformanceProfile profile = PerformanceProfile.builder().fastQSizeGB(10.0).build();
        when(costCalculator.calculate(eq(profile), eq(1.0))).thenReturn(COST);
        Metrics.record(profile, ONE_HOUR, monitor, costCalculator);
        verify(monitor, times(4)).update(metricArgumentCaptor.capture());
    }

    @Test
    public void recordsTotalRuntimeInMillis() {
        Metric runtime = metricArgumentCaptor.getAllValues().get(0);
        assertThat(runtime.name()).isEqualTo(Metrics.BOOTSTRAP + "_SPENT_TIME");
        assertThat(runtime.value()).isEqualTo(ONE_HOUR);
    }

    @Test
    public void recordsCost() {
        Metric runtime = metricArgumentCaptor.getAllValues().get(1);
        assertThat(runtime.name()).isEqualTo(Metrics.COST);
        assertThat(runtime.value()).isEqualTo(COST);
    }

    @Test
    public void recordsFastQSizeGB() {
        Metric runtime = metricArgumentCaptor.getAllValues().get(2);
        assertThat(runtime.name()).isEqualTo(Metrics.FASTQ_SIZE_GB);
        assertThat(runtime.value()).isEqualTo(10.0);
    }

    @Test
    public void recordsCostPerGB() {
        Metric runtime = metricArgumentCaptor.getAllValues().get(3);
        assertThat(runtime.name()).isEqualTo(Metrics.COST_PER_GB);
        assertThat(runtime.value()).isEqualTo(10.0);
    }
}