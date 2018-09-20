package com.hartwig.pipeline.upload;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize
public interface BamMetadata {

    String directory();

    String filename();

    String hash();

    long filesize();

    String bucket();

    static ImmutableBamMetadata.Builder builder() {
        return ImmutableBamMetadata.builder();
    }
}