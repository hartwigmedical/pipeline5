package com.hartwig.pipeline.spark;

import org.immutables.value.Value;

@Value.Immutable
public interface Version {

    @Value.Parameter
    String name();

    static Version of(String name) {
        return ImmutableVersion.of(name);
    }
}
