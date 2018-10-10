package com.hartwig.patient;

import org.immutables.value.Value;

@Value.Immutable
public interface ReferenceGenome {

    @Value.Parameter
    String path();

    static ReferenceGenome of(String path) {
        return ImmutableReferenceGenome.of(path);
    }
}
