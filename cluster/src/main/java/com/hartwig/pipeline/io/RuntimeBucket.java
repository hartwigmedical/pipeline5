package com.hartwig.pipeline.io;

import com.google.cloud.storage.*;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Run;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeBucket {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeBucket.class);

    private final Bucket bucket;

    public static RuntimeBucket from(Storage storage, String sampleName, Arguments arguments) {
        return createBucketIfNeeded(storage, arguments, Run.from(sampleName, arguments));
    }

    public static RuntimeBucket from(Storage storage, String referenceSampleName, String tumorSampleName, Arguments arguments) {
        return createBucketIfNeeded(storage, arguments, Run.from(referenceSampleName, tumorSampleName, arguments));
    }

    @NotNull
    private static RuntimeBucket createBucketIfNeeded(final Storage storage, final Arguments arguments, final Run run) {
        Bucket bucket = storage.get(run.id());
        if (bucket == null) {
            LOGGER.info("Creating runtime bucket [{}] in Google Storage", run.id());
            bucket = storage.create(BucketInfo.newBuilder(run.id())
                    .setStorageClass(StorageClass.REGIONAL)
                    .setLocation(arguments.region())
                    .build());
            LOGGER.info("Creating runtime bucket complete");
        }
        return new RuntimeBucket(bucket);
    }

    private RuntimeBucket(final Bucket bucket) {
        this.bucket = bucket;
    }

    public Bucket bucket() {
        return bucket;
    }

    public String name() {
        return bucket.getName();
    }

    public void cleanup() {
        if (bucket.exists()) {
            for (Blob blob : bucket.list().iterateAll()) {
                blob.delete();
            }
            bucket.delete();
            LOGGER.info("Cleaned up all data in runtime bucket [{}]", bucket.getName());
        }
    }
}
