package com.hartwig.pipeline.storage;

import com.google.cloud.storage.BlobId;

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

    default BlobId asBlobId() {
        String[] splitBucket = bucket().split("/");
        String bucketNoNamespace = splitBucket[0];
        String namespace = splitBucket.length == 2 ? splitBucket[1] : "";
        return BlobId.of(bucketNoNamespace, namespace + "/" + path());
    }

    static GoogleStorageLocation of(String bucket, String path) {
        return of(bucket, path, false);
    }

    static GoogleStorageLocation of(String bucket, String path, boolean isDir) {
        return ImmutableGoogleStorageLocation.of(bucket, path, isDir);
    }
}
