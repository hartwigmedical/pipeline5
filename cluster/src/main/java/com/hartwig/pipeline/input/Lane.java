package com.hartwig.pipeline.input;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableLane.class)
public interface Lane {

    String NOT_APPLICABLE = "NA";

    String laneNumber();

    String firstOfPairPath();

    String secondOfPairPath();

    @Value.Default
    default String flowCellId() {
        return NOT_APPLICABLE;
    }

    static ImmutableLane.Builder builder() {
        return ImmutableLane.builder();
    }
}