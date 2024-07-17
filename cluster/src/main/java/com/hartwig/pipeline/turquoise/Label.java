package com.hartwig.pipeline.turquoise;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableLabel.class)
@JsonDeserialize(as = ImmutableLabel.class)
public interface Label {

    @Value.Parameter
    String name();

    @Value.Parameter
    String value();

    static Label of(final String name, final String value) {
        return ImmutableLabel.of(name, value);
    }
}