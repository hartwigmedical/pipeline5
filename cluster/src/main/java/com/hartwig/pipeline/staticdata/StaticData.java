package com.hartwig.pipeline.staticdata;

import java.util.function.Function;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticData {

    private static final Logger LOGGER = LoggerFactory.getLogger(StaticData.class);

    private final Storage storage;
    private final String sourceBucket;
    private final Function<String, String> alias;

    public StaticData(final Storage storage, final String sourceBucket, final Function<String, String> alias) {
        this.storage = storage;
        this.sourceBucket = sourceBucket;
        this.alias = alias;
    }

    public StaticData(final Storage storage, final String sourceBucket) {
        this(storage, sourceBucket, Function.identity());
    }

    public void copyInto(RuntimeBucket runtimeBucket) {
        Bucket staticDataBucket = storage.get(sourceBucket);
        if (staticDataBucket != null) {
            Page<Blob> blobs = staticDataBucket.list();
            LOGGER.info("Copying static data from [{}] into [{}]", sourceBucket, runtimeBucket.getName());
            for (Blob source : blobs.iterateAll()) {
                BlobId target = BlobId.of(runtimeBucket.bucket().getName(), sourceBucket + "/" + alias.apply(source.getName()));
                storage.copy(Storage.CopyRequest.of(sourceBucket, source.getName(), target));
            }
            LOGGER.info("Copying static data complete");
        } else {
            LOGGER.warn("No bucket found for static data [{}] check that it exists in storage", sourceBucket);
        }
    }
}
