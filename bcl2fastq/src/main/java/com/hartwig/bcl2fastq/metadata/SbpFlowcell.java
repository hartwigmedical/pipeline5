package com.hartwig.bcl2fastq.metadata;

import java.util.Optional;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSbpFlowcell.class)
public interface SbpFlowcell {

    String STATUS_CONVERTED = "Converted";

    Optional<String> convertTime();

    Optional<String> updateTime();

    String name();

    String status();

    int id();

    boolean undet_rds_p_pass();

    static ImmutableSbpFlowcell.Builder builder() {
        return ImmutableSbpFlowcell.builder();
    }

    static ImmutableSbpFlowcell.Builder builderFrom(SbpFlowcell other) {
        return ImmutableSbpFlowcell.builder().from(other);
    }
}