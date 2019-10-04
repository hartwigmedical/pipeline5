package com.hartwig.pipeline.transfer;

import static java.lang.String.format;

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

    default String toUrl() {
        return format("%s://%s/%s", provider(), bucket(), path());
    }

    default String toManifestForm() {
        return format("%.32s %15d %s", md5(), size(), path());
    }
}
