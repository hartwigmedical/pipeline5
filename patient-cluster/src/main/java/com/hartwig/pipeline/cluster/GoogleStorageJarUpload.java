package com.hartwig.pipeline.cluster;

import static java.lang.String.format;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.hartwig.pipeline.bootstrap.Arguments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageJarUpload implements JarUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleStorageJarUpload.class);
    private static final String JAR_BUCKET = "jars-pipeline5";
    private final Storage storage;

    public GoogleStorageJarUpload(final Storage storage) {
        this.storage = storage;
    }

    @Override
    public JarLocation run(Arguments arguments) throws IOException {
        Bucket bucket = storage.get(JAR_BUCKET);
        if (bucket == null) {
            LOGGER.info("JAR bucket [{}] was not present in Google Storage. Creating it.", JAR_BUCKET);
            bucket = storage.create(BucketInfo.newBuilder(JAR_BUCKET)
                    .setStorageClass(StorageClass.REGIONAL)
                    .setLocation(arguments.region())
                    .build());
        }
        String jarName = format("system%s.jar", version(arguments));
        String jarPath = arguments.jarLibDirectory() + jarName;
        String blobLocation = format("gs://%s/%s", JAR_BUCKET, jarName);
        Blob jarBlob = bucket.get(jarName);
        if (jarBlob == null || arguments.forceJarUpload()) {
            LOGGER.info("Uploading jar [{}] into [{}]", jarPath, blobLocation);
            bucket.create(jarName, new FileInputStream(jarPath));
            LOGGER.info("Upload complete");
        }
        return JarLocation.of(blobLocation);
    }

    private static String version(final Arguments arguments) {
        return !arguments.version().isEmpty() ? "-" + arguments.version() : arguments.version();
    }
}
