package com.hartwig.pipeline.execution.dataproc;

import static java.lang.String.format;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.google.cloud.storage.Blob;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleStorageJarUpload implements JarUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleStorageJarUpload.class);
    private static final String JAR_BUCKET = "jars";

    @Override
    public JarLocation run(RuntimeBucket runtimeBucket, Arguments arguments) throws IOException {
        String jarName = format("system%s.jar", version(arguments));
        String jarPath = arguments.jarDirectory() + File.separator + jarName;
        String blobLocation = format("%s/%s", JAR_BUCKET, jarName);
        Blob jarBlob = runtimeBucket.get(blobLocation);
        String blobUri = String.format("gs://%s/%s", runtimeBucket.name(), blobLocation);
        if (jarBlob == null || arguments.forceJarUpload()) {
            LOGGER.info("Uploading jar [{}] into [{}]", jarPath, blobUri);
            runtimeBucket.create(blobLocation, new FileInputStream(jarPath));
            LOGGER.info("Upload complete");
        }
        return JarLocation.of(blobUri);
    }

    private static String version(final Arguments arguments) {
        return !arguments.version().isEmpty() ? "-" + arguments.version() : "";
    }
}
