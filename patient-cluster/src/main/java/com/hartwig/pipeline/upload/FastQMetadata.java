package com.hartwig.pipeline.upload;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableFastQMetadata.class)
@Value.Immutable
public interface FastQMetadata {

    String name_r1();

    String name_r2();

    boolean qc_pass();
}
