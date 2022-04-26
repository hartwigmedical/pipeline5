package com.hartwig.pipeline.transfer;

import java.util.List;
import java.util.function.Consumer;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

public class OutputIterator {
    private final Consumer<Blob> action;
    private final Bucket sourceBucket;

    private OutputIterator(final Consumer<Blob> action, final Bucket sourceBucket) {
        this.action = action;
        this.sourceBucket = sourceBucket;
    }

    public static OutputIterator from(final Consumer<Blob> action, final Bucket sourceBucket) {
        return new OutputIterator(action, sourceBucket);
    }

    public void iterate(final SomaticRunMetadata metadata) {
        find(sourceBucket, metadata.set()).forEach(action);
    }

    private List<Blob> find(final Bucket bucket, final String prefix) {
        return Lists.newArrayList(bucket.list(Storage.BlobListOption.prefix(prefix + "/")).iterateAll());
    }
}
