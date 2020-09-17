package com.hartwig.pipeline.storage;

import org.immutables.value.Value;

@Value.Immutable
public interface GoogleStorageLocation {

    @Value.Parameter
    String bucket();

    @Value.Parameter
    String path();

    @Value.Parameter
    @Value.Default
    default boolean isDirectory() {
        return false;
    }

    default GoogleStorageLocation parent() {
        String[] split = path().split("/");
        return GoogleStorageLocation.of(bucket(), split[split.length - 1], true);
    }

    static GoogleStorageLocation of(String bucket, String path) {
        return of(bucket, path, false);
    }

    static GoogleStorageLocation of(String bucket, String path, boolean isDir) {
        return ImmutableGoogleStorageLocation.of(bucket, path, isDir);
    }
}
