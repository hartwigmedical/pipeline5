package com.hartwig.pipeline.turquoise;

import java.time.ZonedDateTime;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableEvent.class)
@JsonDeserialize(as = ImmutableEvent.class)
@Value.Style(jdkOnly = true)
public interface Event {

    @Value.Parameter
    ZonedDateTime timestamp();

    @Value.Parameter
    String type();

    @Value.Parameter
    List<Subject> subjects();

    @Value.Parameter
    List<Label> labels();

    static Event of(final ZonedDateTime timestamp, final String eventType, final List<Subject> subject, final List<Label> labels) {
        return ImmutableEvent.of(timestamp, eventType, subject, labels);
    }
}