package com.hartwig.pipeline.io.sbp;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableBamMetadata.class)
public interface BamMetadata {

    String directory();

    String filename();

    String hash();

    long filesize();

    String bucket();

    @Value.Default
    default String status() {
        return "Done_PipelineV5";
    }

    static ImmutableBamMetadata.Builder builder() {
        return ImmutableBamMetadata.builder();
    }
}