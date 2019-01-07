package com.hartwig.pipeline.bootstrap;

import org.immutables.value.Value;

@Value.Immutable
public interface Run {

    @Value.Parameter
    String id();

    static Run from(String sampleName, Arguments arguments) {
        String id =
                arguments.runId().map(suffix -> String.format("%s-%s", sampleName.toLowerCase(), suffix)).orElse(sampleName.toLowerCase());
        return ImmutableRun.of(String.format("run-%s", id));
    }
}
