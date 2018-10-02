package com.hartwig.pipeline.metrics;

import java.io.IOException;

import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.hartwig.pipeline.metrics.google.StackdriverMonitor;

public interface Monitor {

    void update(Metric metric);

    static Monitor stackdriver(Run run, String project) {
        try {
            return new StackdriverMonitor(MetricServiceClient.create(), run, project);
        } catch (IOException e) {
            throw new RuntimeException("Could not connect to Google StackDriver logging", e);
        }
    }

    static Monitor noop() {
        return metric -> {
            // do nothing;
        };
    }
}
