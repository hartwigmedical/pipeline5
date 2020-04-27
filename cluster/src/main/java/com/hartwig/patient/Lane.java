package com.hartwig.patient;

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
    default String name() {
        return "lane_" + laneNumber();
    }

    @Value.Default
    default String flowCellId() {
        return NOT_APPLICABLE;
    }

    @Value.Default
    default String index() {
        return NOT_APPLICABLE;
    }

    @Value.Default
    default String suffix() {
        return NOT_APPLICABLE;
    }

    default String recordGroupId() {
        // Assumption is that name follows "sample_lane" format.
        String[] split = name().split("_");
        return String.format("%s_%s_%s_%s_%s", split[0], flowCellId(), index(), split[1], suffix());
    }

    static ImmutableLane.Builder builder() {
        return ImmutableLane.builder();
    }
}