package com.hartwig.pipeline.metadata;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSbpSampleStatusUpdate.class)
@Value.Immutable
public interface SbpSampleStatusUpdate {

    @Value.Parameter
    String status();

    static SbpSampleStatusUpdate of(String status){
        return ImmutableSbpSampleStatusUpdate.of(status);
    }
}
