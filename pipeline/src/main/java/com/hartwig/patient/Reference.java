package com.hartwig.patient;

import com.hartwig.pipeline.Configuration;

import org.immutables.value.Value;

@Value.Immutable
public interface Reference {

    @Value.Parameter
    String path();

    static Reference from(Configuration configuration) {
        return ImmutableReference.of(configuration.referencePath());
    }
}
