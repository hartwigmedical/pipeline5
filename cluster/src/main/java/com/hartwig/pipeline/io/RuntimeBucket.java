package com.hartwig.pipeline.io;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.bootstrap.Run;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeBucket {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeBucket.class);

    public Bucket bucket() {
        return bucket;
    }

    private final Bucket bucket;

    public RuntimeBucket(final Bucket bucket) {
        this.bucket = bucket;
    }

    public static RuntimeBucket from(Storage storage, String sampleName, Arguments arguments) {
        Run run = Run.from(sampleName, arguments);
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

    public void cleanup() {
        if (bucket.exists()) {
            for (Blob blob : bucket.list().iterateAll()) {
                blob.delete();
            }
            bucket.delete();
            LOGGER.info("Cleaned up all data in runtime bucket [{}]", bucket.getName());
        }
    }

    public String getName() {
        return bucket.getName();
    }
}
