package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableSbpFileMetadata.class)
@JsonDeserialize(as = ImmutableSbpFileMetadata.class)
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
