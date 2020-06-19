package com.hartwig.pipeline.turquoise;

import java.time.LocalDateTime;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableEvent.class)
interface Event {

    @Value.Parameter
    LocalDateTime timestamp();

    @Value.Parameter
    String type();

    @Value.Parameter
    List<Subject> subjects();

    static Event of(LocalDateTime timestamp, String eventType, List<Subject> subject) {
        return ImmutableEvent.of(timestamp, eventType, subject);
    }
}