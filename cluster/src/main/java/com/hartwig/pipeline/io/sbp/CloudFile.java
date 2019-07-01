package com.hartwig.pipeline.io.sbp;

import javax.annotation.Nullable;

import org.immutables.value.Value;

@Value.Immutable
public interface CloudFile {
    String provider();
    String bucket();
    String path();

    @Nullable
    String md5();

    @Nullable
    Long size();

    static ImmutableCloudFile.Builder builder() {
        return ImmutableCloudFile.builder();
    }
}
