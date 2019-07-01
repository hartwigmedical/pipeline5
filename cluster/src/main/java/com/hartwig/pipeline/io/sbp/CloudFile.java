package com.hartwig.pipeline.io.sbp;

import org.immutables.value.Value;

@Value.Immutable
public interface CloudFile {
    String provider();
    String bucket();
    String path();
    String md5();
    Long size();

    static ImmutableCloudFile.Builder builder() {
        return ImmutableCloudFile.builder();
    }
}
