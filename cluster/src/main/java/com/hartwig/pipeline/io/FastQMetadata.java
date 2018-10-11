package com.hartwig.pipeline.io;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

@JsonDeserialize(as = ImmutableFastQMetadata.class)
@Value.Immutable
public interface FastQMetadata {

    String name_r1();

    String name_r2();

    @Nullable
    String bucket();

    boolean qc_pass();
}
