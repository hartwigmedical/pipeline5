package com.hartwig.patient;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableReferenceTumorPair.class)
@Value.Immutable
public interface ReferenceTumorPair {

    Optional<Sample> reference();

    Optional<Sample> tumor();
}