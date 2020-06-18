package com.hartwig.pipeline.turquoise;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableEventType.class)
interface EventType {

    @Value.Parameter
    String name();
}
