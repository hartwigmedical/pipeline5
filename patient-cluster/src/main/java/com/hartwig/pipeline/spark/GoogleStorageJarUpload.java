package com.hartwig.pipeline.spark;

import static java.lang.String.format;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageJarUpload implements JarUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleStorageJarUpload.class);
    static final String JAR_BUCKET = "jars-pipeline5";
    private final Storage storage;
    private final String region;
    private final String libDirectory;
    private final boolean force;

    public GoogleStorageJarUpload(final Storage storage, final String region, final String libDirectory, final boolean force) {
        this.storage = storage;
        this.region = region;
        this.libDirectory = libDirectory;
        this.force = force;
    }

    @Override
    public JarLocation run(final Version version) throws IOException {
        Bucket bucket = storage.get(JAR_BUCKET);
        if (bucket == null) {
            LOGGER.info("JAR bucket [{}] was not present in Google Storage. Creating it.", JAR_BUCKET);
            bucket = storage.create(BucketInfo.newBuilder(JAR_BUCKET).setStorageClass(StorageClass.REGIONAL).setLocation(region).build());
        }
        String jarName = format("system-%s.jar", version.name());
        String jarPath = libDirectory + jarName;
        String blobLocation = format("gs://%s/%s", JAR_BUCKET, jarName);
        Blob jarBlob = bucket.get(jarName);
        if (jarBlob == null || force) {
            LOGGER.info("Uploading jar [{}] into [{}]", jarPath, blobLocation);
            bucket.create(jarName, new FileInputStream(jarPath));
            LOGGER.info("Upload complete");
        }
        return JarLocation.of(blobLocation);
    }
}
