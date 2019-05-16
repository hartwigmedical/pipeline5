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
    private final String commonResourcesBucket;
    private final String name;
    private final Function<String, String> alias;

    public Resource(final Storage storage, final String commonResourcesBucket, final String name, final Function<String, String> alias) {
        this.storage = storage;
        this.commonResourcesBucket = commonResourcesBucket;
        this.name = name;
        this.alias = alias;
    }

    public Resource(final Storage storage, final String commonResourcesBucket, final String name) {
        this(storage, commonResourcesBucket, name, Function.identity());
    }

    public ResourceLocation copyInto(RuntimeBucket runtimeBucket) {
        ImmutableResourceLocation.Builder locationBuilder = ResourceLocation.builder();
        locationBuilder.bucket(runtimeBucket.name() + "/" + name);
        Bucket staticDataBucket = storage.get(commonResourcesBucket);
        if (staticDataBucket != null) {
            Page<Blob> blobs = staticDataBucket.list(Storage.BlobListOption.prefix(name));
            String fullyQualifiedSourceBucket = commonResourcesBucket + "/" + name;
            LOGGER.debug("Copying static data from [{}] into [{}]", fullyQualifiedSourceBucket, runtimeBucket.name());
            for (Blob source : blobs.iterateAll()) {
                String aliased = alias.apply(source.getName());
                String[] targetPath = aliased.split("/");
                String targetBlob = String.format("%s/%s", name, targetPath[targetPath.length - 1]);
                runtimeBucket.copyInto(commonResourcesBucket, source.getName(), targetBlob);
                locationBuilder.addFiles(aliased);
            }
            LOGGER.debug("Copying static data complete");
        } else {
            LOGGER.warn("No bucket found for static data [{}] check that it exists in storage", name);
        }
        return locationBuilder.build();
    }
}
