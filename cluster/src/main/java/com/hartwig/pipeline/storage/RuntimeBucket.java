package com.hartwig.pipeline.storage;

import java.io.InputStream;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Run;
import com.hartwig.pipeline.metadata.RunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeBucket {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeBucket.class);

    private final Storage storage;
    private final Bucket bucket;
    private final String namespace;
    private final Run run;

    public static RuntimeBucket from(final Storage storage, final String namespace, final RunMetadata metadata,
            final Arguments arguments) {
        return createBucketIfNeeded(storage, namespace, arguments, Run.from(metadata, arguments));
    }

    private synchronized static RuntimeBucket createBucketIfNeeded(final Storage storage, final String namespace, final Arguments arguments,
            final Run run) {
        Bucket bucket = storage.get(run.id());
        if (bucket == null) {
            LOGGER.debug("Creating runtime bucket [{}] in Google Storage", run.id());
            BucketInfo.Builder builder =
                    BucketInfo.newBuilder(run.id()).setStorageClass(StorageClass.REGIONAL).setLocation(arguments.region());
            arguments.cmek().ifPresent(key -> {
                LOGGER.info("Using CMEK key [{}] to encrypt all buckets", key);
                builder.setDefaultKmsKeyName(String.format("projects/%s/locations/%s/keyRings/%s/cryptoKeys/%s",
                        arguments.project(),
                        arguments.region(),
                        arguments.project(),
                        key));
            });
            bucket = storage.create(builder.build());
        }
        return new RuntimeBucket(storage, bucket, namespace, run);
    }

    public String getNamespace() {
        return namespace;
    }

    public Blob get(String blobName) {
        return bucket.get(namespace(blobName));
    }

    private String namespace(final String blobName) {
        String[] blobPath = blobName.split("/");
        return !blobPath[0].equals(namespace) ? namespace + (blobName.startsWith("/") ? blobName : ("/" + blobName)) : blobName;
    }

    public void create(String blobName, byte[] content) {
        bucket.create(namespace(blobName), content);
    }

    public void create(String blobName, InputStream content) {
        bucket.create(namespace(blobName), content);
    }

    public List<Blob> list() {
        return Lists.newArrayList(bucket.list(Storage.BlobListOption.prefix(namespace)).iterateAll());
    }

    public List<Blob> list(String prefix) {
        return Lists.newArrayList(bucket.list(Storage.BlobListOption.prefix(namespace(prefix))).iterateAll());
    }

    public void delete(String prefix) {
        list(prefix).forEach(Blob::delete);
    }

    public void copyInto(String sourceBucket, String sourceBlobName, String targetBlobName) {
        BlobInfo targetBlobInfo = BlobInfo.newBuilder(bucket.getName(), namespace(targetBlobName)).build();
        storage.copy(Storage.CopyRequest.of(sourceBucket, sourceBlobName, targetBlobInfo)).getResult();
    }

    public void copyOutOf(String sourceBlobName, String targetBucket, String targetBlob) {
        BlobInfo targetBlobInfo = BlobInfo.newBuilder(targetBucket, targetBlob).build();
        storage.copy(Storage.CopyRequest.of(bucket.getName(), namespace(sourceBlobName), targetBlobInfo)).getResult();
    }

    void compose(List<String> sources, String target) {
        storage.compose(Storage.ComposeRequest.of(bucket.getName(), sources, namespace(target)));
    }

    private RuntimeBucket(final Storage storage, final Bucket bucket, final String namespace, final Run run) {
        this.storage = storage;
        this.bucket = bucket;
        this.namespace = namespace;
        this.run = run;
    }

    public String name() {
        return bucket.getName() + "/" + namespace;
    }

    public String runId() {
        return run.id();
    }

    @Override
    public String toString() {
        return String.format("runtime bucket [%s]", name());
    }
}
