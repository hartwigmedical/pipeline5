package com.hartwig.pipeline.metadata;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSbpStatusUpdate.class)
@Value.Immutable
public interface SbpStatusUpdate {

    @Value.Parameter
    String status();

    static SbpStatusUpdate of (String status){
        return ImmutableSbpStatusUpdate.of(status);
    }
}
