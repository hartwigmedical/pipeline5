package com.hartwig.pipeline.io.sbp;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableSampleMetadata.class)
@Value.Immutable
public interface SampleMetadata {

    String barcode();
}
