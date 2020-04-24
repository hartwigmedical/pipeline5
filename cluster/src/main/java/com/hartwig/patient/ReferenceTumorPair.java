package com.hartwig.patient;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableReferenceTumorPair.class)
@Value.Immutable
public interface ReferenceTumorPair {

    Sample reference();

    Sample tumor();
}
