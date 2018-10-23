package com.hartwig.pipeline.metrics;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

public class MetricsTimeline {

    private final Map<Stage, Long> started = new HashMap<>();
    private final Clock clock;
    private final Metrics metrics;

    public MetricsTimeline(final Clock clock, final Metrics metrics) {
        this.clock = clock;
        this.metrics = metrics;
    }

    public MetricsTimeline start(Stage stage) {
        started.put(stage, clock.millis());
        return this;
    }

    public void stop(Stage stage) {
        if (started.containsKey(stage)) {
            long startTime = started.get(stage);
            long endTime = clock.millis();
            long runtimeMillis = endTime - startTime;
            metrics.record(stage.name(), stage.performanceProfile(), runtimeMillis);
        } else {
            throw new IllegalStateException(
                    "[%s] was never started or already stopped. Check your code to ensure you've called start " + "before stop.");
        }
    }
}
