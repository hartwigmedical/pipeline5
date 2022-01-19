package com.hartwig.pipeline.storage;

import java.io.InputStream;
import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.CommonArguments;
import com.hartwig.pipeline.alignment.Run;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.metadata.RunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeBucket {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeBucket.class);

    private final Storage storage;
    private final Bucket bucket;
    private final String namespace;
    private final String runId;

    private RuntimeBucket(final Storage storage, final Bucket bucket, final String namespace, final String runId) {
        this.storage = storage;
        this.bucket = bucket;
        this.namespace = namespace;
        this.runId = runId;
    }

    public static RuntimeBucket from(final Storage storage, final String name, final String namespace, final CommonArguments arguments,
            final Labels labels) {
        return createBucketIfNeeded(storage, namespace, arguments, name, labels);
    }

    public static RuntimeBucket from(final Storage storage, final String namespace, final RunMetadata metadata,
            final CommonArguments arguments, final Labels labels) {
        return createBucketIfNeeded(storage, namespace, arguments, Run.from(metadata, arguments).id(), labels);
    }

    private synchronized static RuntimeBucket createBucketIfNeeded(final Storage storage, final String namespace,
            final CommonArguments arguments, final String runId, final Labels labels) {
        Bucket bucket = storage.get(runId);
        if (bucket == null) {
            LOGGER.info("Creating runtime bucket [{}] in Google Storage", runId);
            BucketInfo.Builder builder = BucketInfo.newBuilder(runId)
                    .setStorageClass(StorageClass.REGIONAL)
                    .setLocation(arguments.region())
                    .setLabels(labels.asMap());
            arguments.cmek().ifPresent(builder::setDefaultKmsKeyName);
            bucket = storage.create(builder.build());
        }
        return new RuntimeBucket(storage, bucket, namespace, runId);
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

    public void copyOutOf(String sourceBlobName, String targetBucket, String targetBlob) {
        BlobInfo targetBlobInfo = BlobInfo.newBuilder(targetBucket, targetBlob).build();
        storage.copy(Storage.CopyRequest.of(bucket.getName(), namespace(sourceBlobName), targetBlobInfo)).getResult();
    }

    public void copyInto(String sourceBucketName, String sourceBlobName, String targetBlobName) {
        BlobId sourceBlobId = BlobId.of(sourceBucketName, sourceBlobName);
        BlobId targetBlobId = BlobId.of(bucket.getName(), namespace(targetBlobName));
        storage.copy(Storage.CopyRequest.of(sourceBlobId, targetBlobId)).getResult();
    }

    public String name() {
        return bucket.getName() + "/" + namespace;
    }

    public String runId() {
        return runId;
    }

    public Bucket getUnderlyingBucket() {
        return bucket;
    }

    @Override
    public String toString() {
        return String.format("runtime bucket [%s]", name());
    }
}
