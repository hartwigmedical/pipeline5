package com.hartwig.pipeline.metadata;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSbpSample.class)
@JsonSerialize(as = ImmutableSbpSample.class)
@Value.Immutable
public interface SbpSample {

    String id();

    String barcode();
}
