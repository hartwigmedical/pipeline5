package com.hartwig.pipeline.resource;

import java.util.function.Function;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Resource {

    private static final Logger LOGGER = LoggerFactory.getLogger(Resource.class);

    private final Storage storage;
    private final String sourceBucket;
    private final String targetBucket;
    private final Function<String, String> alias;

    Resource(final Storage storage, final String sourceBucket, final String targetBucket, final Function<String, String> alias) {
        this.storage = storage;
        this.sourceBucket = sourceBucket;
        this.targetBucket = targetBucket;
        this.alias = alias;
    }

    Resource(final Storage storage, final String sourceBucket, final String targetBucket) {
        this(storage, sourceBucket, targetBucket, Function.identity());
    }

    public ResourceLocation copyInto(RuntimeBucket runtimeBucket) {
        ImmutableResourceLocation.Builder locationBuilder = ResourceLocation.builder();
        locationBuilder.bucket(targetBucket);
        Bucket staticDataBucket = storage.get(sourceBucket);
        if (staticDataBucket != null) {
            Page<Blob> blobs = staticDataBucket.list();
            LOGGER.info("Copying static data from [{}] into [{}]", sourceBucket, runtimeBucket.name());
            for (Blob source : blobs.iterateAll()) {
                String aliased = alias.apply(source.getName());
                BlobId target = BlobId.of(runtimeBucket.bucket().getName(), targetBucket + "/" + aliased);
                storage.copy(Storage.CopyRequest.of(sourceBucket, source.getName(), target));
                locationBuilder.addFiles(target.getName());
            }
            LOGGER.info("Copying static data complete");
        } else {
            LOGGER.warn("No bucket found for static data [{}] check that it exists in storage", sourceBucket);
        }
        return locationBuilder.build();
    }

    public String getTargetBucket() {
        return targetBucket;
    }
}
