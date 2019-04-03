package com.hartwig.pipeline.metrics;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import com.hartwig.pipeline.cluster.SparkJobDefinition;

public class MetricsTimeline {

    private final Map<SparkJobDefinition, Long> started = new HashMap<>();
    private final Clock clock;
    private final Metrics metrics;

    public MetricsTimeline(final Clock clock, final Metrics metrics) {
        this.clock = clock;
        this.metrics = metrics;
    }

    public void start(SparkJobDefinition jobDefinition) {
        started.put(jobDefinition, clock.millis());
    }

    public void stop(SparkJobDefinition jobDefinition) {
        if (started.containsKey(jobDefinition)) {
            long startTime = started.get(jobDefinition);
            long endTime = clock.millis();
            long runtimeMillis = endTime - startTime;
            metrics.record(jobDefinition.name(), jobDefinition.performanceProfile(), runtimeMillis);
        } else {
            throw new IllegalStateException(String.format(
                    "[%s] was never started or already stopped. Check your code to ensure you've called start before stop.",
                    jobDefinition));
        }
    }
}
