package com.hartwig.bcl2fastq.metadata;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableSbpFlowcell.class)
public interface SbpFlowcell {

    String convertTime();

    String updateTime();

    String name();

    String status();

    String id();

    boolean undet_rds_p_pass();

    static ImmutableSbpFlowcell.Builder builderFrom(SbpFlowcell other) {
        return ImmutableSbpFlowcell.builder().from(other);
    }
}