package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSbpSample.class)
@JsonSerialize(as = ImmutableSbpSample.class)
@Value.Immutable
public interface SbpSample {

    int id();

    String name();

    String barcode();

    String type();
}
