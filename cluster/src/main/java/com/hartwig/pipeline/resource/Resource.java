package com.hartwig.pipeline.resource;

import java.util.function.Function;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Resource {

    private static final Logger LOGGER = LoggerFactory.getLogger(Resource.class);

    private final Storage storage;
    private final String sourceBucket;
    private final String targetPath;
    private final Function<String, String> alias;

    public Resource(final Storage storage, final String sourceBucket, final String targetPath, final Function<String, String> alias) {
        this.storage = storage;
        this.sourceBucket = sourceBucket;
        this.targetPath = targetPath;
        this.alias = alias;
    }

    public Resource(final Storage storage, final String sourceBucket, final String targetPath) {
        this(storage, sourceBucket, targetPath, Function.identity());
    }

    public ResourceLocation copyInto(RuntimeBucket runtimeBucket) {
        ImmutableResourceLocation.Builder locationBuilder = ResourceLocation.builder();
        locationBuilder.bucket(runtimeBucket.name() + "/" + targetPath);
        Bucket staticDataBucket = storage.get(sourceBucket);
        if (staticDataBucket != null) {
            Page<Blob> blobs = staticDataBucket.list();
            LOGGER.info("Copying static data from [{}] into [{}]", sourceBucket, runtimeBucket.name());
            for (Blob source : blobs.iterateAll()) {
                String aliased = alias.apply(source.getName());
                String targetBlob = String.format("%s/%s", targetPath, aliased);
                runtimeBucket.copyInto(sourceBucket, source.getName(), targetBlob);
                locationBuilder.addFiles(aliased);
            }
            LOGGER.info("Copying static data complete");
        } else {
            LOGGER.warn("No bucket found for static data [{}] check that it exists in storage", sourceBucket);
        }
        return locationBuilder.build();
    }
}
