package com.hartwig.pipeline.io.sbp;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSbpFileMetadata.class)
public interface SbpFileMetadata {
    int run_id();

    String filename();

    String hash();

    String directory();

    long filesize();

    static ImmutableSbpFileMetadata.Builder builder() {
        return ImmutableSbpFileMetadata.builder();
    }
}
