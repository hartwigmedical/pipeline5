package com.hartwig.bcl2fastq.metadata;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSbpLane.class)
@JsonDeserialize(as = ImmutableSbpLane.class)
public interface SbpLane {

    Optional<Integer> id();

    int flowcell_id();

    String name();

    @Value.Default
    default long yld() {
        return -1;
    }

    @Value.Default
    default double q30() {
        return -1;
    }

    static ImmutableSbpLane.Builder builder() {
        return ImmutableSbpLane.builder();
    }
}
