package com.hartwig.pipeline.cluster;

import org.immutables.value.Value;

@Value.Immutable
public interface SparkJobDefinition {

    @Value.Parameter
    String mainClass();

    @Value.Parameter
    String jarLocation();

    static SparkJobDefinition of(String mainClass, String jarLocation) {
        return ImmutableSparkJobDefinition.of(mainClass, jarLocation);
    }
}
