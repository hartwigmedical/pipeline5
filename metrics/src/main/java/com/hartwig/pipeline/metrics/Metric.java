package com.hartwig.pipeline.metrics;

import java.time.LocalDateTime;

import org.immutables.value.Value;

@Value.Immutable
public interface Metric {

    @Value.Parameter
    LocalDateTime timestamp();

    @Value.Parameter
    String name();

    @Value.Parameter
    double value();

    static Metric spentTime(String name, long value) {
        return of(name + "_SPENT_TIME", value);
    }

    static Metric of(String name, double value) {
        return ImmutableMetric.of(LocalDateTime.now(), name, value);
    }
}
