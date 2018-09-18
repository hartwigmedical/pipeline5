package com.hartwig.pipeline.bootstrap;

import com.hartwig.patient.Sample;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String name();

    @Value.Parameter
    String id();

    static Run from(Sample sample) {
        return ImmutableRun.of(String.format("run-%s", sample.name().toLowerCase()), "1");
    }
}
