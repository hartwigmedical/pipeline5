package com.hartwig.pipeline.metrics.google;

import java.time.ZoneId;

import com.google.api.MetricDescriptor;
import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.common.collect.ImmutableMap;
import com.google.monitoring.v3.CreateMetricDescriptorRequest;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.util.Timestamps;
import com.hartwig.pipeline.metrics.Metric;
import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StackdriverMonitor implements Monitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(StackdriverMonitor.class);
    private static final MonitoredResource GLOBAL_MONITORED_RESOURCE = MonitoredResource.newBuilder().setType("global").build();
    static final String VERSION = "version";
    static final String ID = "id";
    static final String CUSTOM_DOMAIN = "custom.googleapis.com";
    private final MetricServiceClient client;
    private final Run run;
    private final ProjectName projectName;

    public StackdriverMonitor(final MetricServiceClient client, final Run run, final String project) {
        this.client = client;
        this.run = run;
        this.projectName = ProjectName.of(project);
    }

    @Override
    public void update(final Metric metric) {

        try {
            MetricDescriptor descriptor = MetricDescriptor.newBuilder()
                    .setDisplayName(metric.name())
                    .setType(String.format("%s/%s", CUSTOM_DOMAIN, metric.name()))
                    .setMetricKind(MetricDescriptor.MetricKind.GAUGE)
                    .setValueType(MetricDescriptor.ValueType.DOUBLE)
                    .build();
            CreateMetricDescriptorRequest metricDescriptorRequest =
                    CreateMetricDescriptorRequest.newBuilder().setMetricDescriptor(descriptor).setName(projectName.toString()).build();
            client.createMetricDescriptor(metricDescriptorRequest);

            com.google.api.Metric googleMetric = com.google.api.Metric.newBuilder()
                    .setType(descriptor.getType())
                    .putAllLabels(ImmutableMap.of(VERSION, run.version(), ID, run.id()))
                    .build();
            TypedValue metricValue = TypedValue.newBuilder().setDoubleValue(metric.value()).build();
            Point singlePoint = Point.newBuilder()
                    .setInterval(TimeInterval.newBuilder()
                            .setEndTime(Timestamps.fromMillis(metric.timestamp().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()))
                            .build())
                    .setValue(metricValue)
                    .build();
            TimeSeries singlePointTimeSeries =
                    TimeSeries.newBuilder().setResource(GLOBAL_MONITORED_RESOURCE).setMetric(googleMetric).addPoints(singlePoint).build();
            CreateTimeSeriesRequest request =
                    CreateTimeSeriesRequest.newBuilder().setName(projectName.toString()).addTimeSeries(singlePointTimeSeries).build();
            client.createTimeSeries(request);
        } catch (Exception e) {
            LOGGER.error("Could not post metrics to StackDriver. See following exception for more detail", e);
        }
    }
}
