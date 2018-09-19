package com.hartwig.pipeline.bootstrap;

import java.time.LocalDateTime;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.patient.Sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuntimeBucket {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeBucket.class);

    public Bucket bucket() {
        return bucket;
    }

    private final Bucket bucket;

    private RuntimeBucket(final Bucket bucket) {
        this.bucket = bucket;
    }

    public static RuntimeBucket from(Storage storage, Sample sample, Arguments arguments) {
        Run run = Run.from(sample, arguments, LocalDateTime.now());
        Bucket bucket = storage.get(run.id());
        if (bucket == null) {
            LOGGER.info("Creating runtime bucket [{}] in Google Storage", run.id());
            bucket = storage.create(BucketInfo.newBuilder(run.id())
                    .setStorageClass(StorageClass.REGIONAL)
                    .setLocation(arguments.region())
                    .build());
        }
        return new RuntimeBucket(bucket);
    }

    void cleanup() {
        for (Blob blob : bucket.list().iterateAll()) {
            blob.delete();
        }
        bucket.delete();
        LOGGER.info("Cleaned up all data in runtime bucket [{}]", bucket.getName());
    }

    public String getName() {
        return bucket.getName();
    }
}
