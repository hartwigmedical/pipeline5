package com.hartwig.pipeline.turquoise;

import java.time.ZonedDateTime;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableEvent.class)
interface Event {

    @Value.Parameter
    ZonedDateTime timestamp();

    @Value.Parameter
    String type();

    @Value.Parameter
    List<Subject> subjects();

    @Value.Parameter
    List<Label> labels();

    static Event of(ZonedDateTime timestamp, String eventType, List<Subject> subject, List<Label> labels) {
        return ImmutableEvent.of(timestamp, eventType, subject, labels);
    }
}