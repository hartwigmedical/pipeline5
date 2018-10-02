package com.hartwig.pipeline.metrics.google;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.api.MetricDescriptor;
import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.CreateMetricDescriptorRequest;
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
    private static final String METRIC_TYPE = "metric";
    private static final String CUSTOM_TYPE = StackdriverMonitor.CUSTOM_DOMAIN + "/" + METRIC_TYPE;
    private static final Metric METRIC = Metric.of(METRIC_TYPE, 1.0);
    private MetricServiceClient mockClient;
    private static final String VERSION = "version";
    private static final String ID = "id";
    private static final Run RUN = Run.of(VERSION, ID);
    private Monitor victim;
    private ArgumentCaptor<CreateTimeSeriesRequest> timeSeriesRequestArgumentCaptor;
    private ArgumentCaptor<CreateMetricDescriptorRequest> createMetricDescriptorRequestArgumentCaptor;

    @Before
    public void setUp() throws Exception {
        mockClient = mock(MetricServiceClient.class);
        victim = new StackdriverMonitor(mockClient, RUN, PROJECT);
        timeSeriesRequestArgumentCaptor = ArgumentCaptor.forClass(CreateTimeSeriesRequest.class);
        createMetricDescriptorRequestArgumentCaptor = ArgumentCaptor.forClass(CreateMetricDescriptorRequest.class);
    }

    @Test
    public void createsMetricDescriptor() {
        victim.update(METRIC);
        verify(mockClient).createMetricDescriptor(createMetricDescriptorRequestArgumentCaptor.capture());
        CreateMetricDescriptorRequest result = createMetricDescriptorRequestArgumentCaptor.getValue();
        assertThat(result.getName()).isEqualTo("projects/" + PROJECT);
        assertThat(result.getMetricDescriptor().getType()).isEqualTo(CUSTOM_TYPE);
        assertThat(result.getMetricDescriptor().getMetricKind()).isEqualTo(MetricDescriptor.MetricKind.GAUGE);
        assertThat(result.getMetricDescriptor().getValueType()).isEqualTo(MetricDescriptor.ValueType.DOUBLE);
    }

    @Test
    public void addsMetricsToGlobalMonitoredResource() {
        victim.update(METRIC);
        verify(mockClient).createTimeSeries(timeSeriesRequestArgumentCaptor.capture());
        CreateTimeSeriesRequest result = timeSeriesRequestArgumentCaptor.getValue();
        assertThat(result.getName()).isEqualTo("projects/" + PROJECT);
        assertThat(result.getTimeSeriesCount()).isEqualTo(1);
        MonitoredResource resource = result.getTimeSeries(0).getResource();
        assertThat(resource.getType()).isEqualTo("global");
    }

    @Test
    public void addsSinglePointTimeSeriesToMonitoredResource() {
        victim.update(METRIC);
        verify(mockClient).createTimeSeries(timeSeriesRequestArgumentCaptor.capture());
        CreateTimeSeriesRequest result = timeSeriesRequestArgumentCaptor.getValue();
        TimeSeries resultTimeSeries = result.getTimeSeries(0);
        assertThat(resultTimeSeries.getMetric().getType()).isEqualTo(CUSTOM_TYPE);
        assertThat(resultTimeSeries.getPointsCount()).isEqualTo(1);
        Point resultPoint = resultTimeSeries.getPoints(0);
        assertThat(resultPoint.getValue().getDoubleValue()).isEqualTo(1.0);
    }

    @Test
    public void addsLabelsForRunAndVersionToMetric() {
        victim.update(METRIC);
        verify(mockClient).createTimeSeries(timeSeriesRequestArgumentCaptor.capture());
        CreateTimeSeriesRequest result = timeSeriesRequestArgumentCaptor.getValue();
        assertThat(result.getTimeSeries(0).getMetric().getLabelsMap().get(StackdriverMonitor.VERSION)).isEqualTo(VERSION);
        assertThat(result.getTimeSeries(0).getMetric().getLabelsMap().get(StackdriverMonitor.ID)).isEqualTo(ID);
    }
}