package com.hartwig.pipeline.metadata;

import com.hartwig.patient.Sample;

import org.immutables.value.Value;

@Value.Immutable
public interface SetMetadata {

    @Value.Parameter
    String setName();

    @Value.Parameter
    Sample tumor();

    @Value.Parameter
    Sample reference();

    static SetMetadata of(String setName, Sample tumor, Sample reference) {
        return ImmutableSetMetadata.of(setName, tumor, reference);
    }
}
