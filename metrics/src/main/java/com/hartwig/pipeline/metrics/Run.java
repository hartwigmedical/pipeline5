package com.hartwig.pipeline.metrics;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String version();

    @Value.Parameter
    String id();

    static Run of(String version, String id) {
        return ImmutableRun.of(version, id);
    }
}
