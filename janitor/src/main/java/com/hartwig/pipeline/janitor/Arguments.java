package com.hartwig.pipeline.janitor;

import org.immutables.value.Value;

@Value.Immutable
public interface Arguments {

    String project();

    String region();

    int intervalInSeconds();

    String privateKeyPath();

    static ImmutableArguments.Builder builder() {
        return ImmutableArguments.builder();
    }

    static Arguments defaults() {
        return JanitorOptions.from(new String[] {}).orElseThrow(() -> new IllegalStateException("Could not build default arguments"));
    }
}
