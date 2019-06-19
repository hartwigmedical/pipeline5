package com.hartwig.pipeline.metadata;

import org.immutables.value.Value;

@Value.Immutable
public interface SbpStatusUpdate {

    @Value.Parameter
    String status();

    static SbpStatusUpdate of (String status){
        return ImmutableSbpStatusUpdate.of(status);
    }
}
