package com.hartwig.pipeline.metrics.google;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.TimeSeries;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class StackdriverMonitorTest {

    private static final String PROJECT = "project";
    private static final Metric METRIC = Metric.of("metric", 1.0);
    private MetricServiceClient mockClient;
    private static final String VERSION = "version";
    private static final String ID = "id";
    private static final Run RUN = Run.of(VERSION, ID);
    private Monitor victim;
    private ArgumentCaptor<CreateTimeSeriesRequest> captor;

    @Before
    public void setUp() throws Exception {
        mockClient = mock(MetricServiceClient.class);
        victim = new StackdriverMonitor(mockClient, RUN, PROJECT);
        captor = ArgumentCaptor.forClass(CreateTimeSeriesRequest.class);
    }

    @Test
    public void addsMetricsToMonitoredResourcePerRun() {
        victim.update(METRIC);
        verify(mockClient).createTimeSeries(captor.capture());
        CreateTimeSeriesRequest result = captor.getValue();
        assertThat(result.getName()).isEqualTo("projects/" + PROJECT);
        assertThat(result.getTimeSeriesCount()).isEqualTo(1);
        MonitoredResource resource = result.getTimeSeries(0).getResource();
        assertThat(resource.getType()).isEqualTo("global");
        assertThat(resource.getLabelsMap().get(StackdriverMonitor.VERSION)).isEqualTo(VERSION);
        assertThat(resource.getLabelsMap().get(StackdriverMonitor.ID)).isEqualTo(ID);
    }

    @Test
    public void addsSinglePointTimeSeriesToMonitoredResource() {
        victim.update(METRIC);
        verify(mockClient).createTimeSeries(captor.capture());
        CreateTimeSeriesRequest result = captor.getValue();
        TimeSeries resultTimeSeries = result.getTimeSeries(0);
        assertThat(resultTimeSeries.getMetric().getType()).isEqualTo("metric");
        assertThat(resultTimeSeries.getPointsCount()).isEqualTo(1);
        Point resultPoint = resultTimeSeries.getPoints(0);
        assertThat(resultPoint.getValue().getDoubleValue()).isEqualTo(1.0);
    }
}