package com.hartwig.pipeline.bootstrap;

import java.util.function.Function;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StaticData {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticData.class);

    private final Storage storage;
    private final String sourceBucket;
    private final Function<String, String> alias;

    StaticData(final Storage storage, final String sourceBucket, final Function<String, String> alias) {
        this.storage = storage;
        this.sourceBucket = sourceBucket;
        this.alias = alias;
    }

    StaticData(final Storage storage, final String sourceBucket) {
        this(storage, sourceBucket, Function.identity());
    }

    void copyInto(RuntimeBucket runtimeBucket) {
        Bucket staticDataBucket = storage.get(sourceBucket);
        if (staticDataBucket != null) {
            Page<Blob> blobs = staticDataBucket.list();
            for (Blob source : blobs.iterateAll()) {
                String sourcePath = sourceBucket + "/" + source.getName();
                BlobId target = BlobId.of(runtimeBucket.bucket().getName(), sourceBucket + "/" + alias.apply(source.getName()));
                LOGGER.info("Copying static data from [{}] into [{}]", sourcePath, target);
                storage.copy(Storage.CopyRequest.of(sourceBucket, source.getName(), target));
            }
        } else {
            LOGGER.warn("No bucket found for static data [{}] check that it exists in storage", sourceBucket);
        }
    }
}
