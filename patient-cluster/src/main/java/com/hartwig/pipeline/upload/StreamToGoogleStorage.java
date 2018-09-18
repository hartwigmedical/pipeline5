package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

import com.google.cloud.storage.Blob;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.RuntimeBucket;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamToGoogleStorage implements SampleUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamToGoogleStorage.class);
    private static final String SAMPLE_DIRECTORY = "samples/";
    private final Function<File, InputStream> dataStreamSupplier;

    public StreamToGoogleStorage(final Function<File, InputStream> dataStreamSupplier) {
        this.dataStreamSupplier = dataStreamSupplier;
    }

    @Override
    public void run(Sample sample, RuntimeBucket runtimeBucket) throws IOException {
        LOGGER.info("Uploading sample [{}] into [{}]", sample.name(), runtimeBucket.bucket().getName());
        if (sampleDirectoryNotExists(runtimeBucket)) {
            uploadSample(runtimeBucket, sample, file -> singleSampleFile(sample, file));
        } else {
            LOGGER.info("Sample [{}] was already present in [{}]. Skipping upload.", sample.name(), runtimeBucket.bucket().getName());
        }
        LOGGER.info("Upload complete");
    }

    private boolean sampleDirectoryNotExists(final RuntimeBucket runtimeBucket) {
        boolean samplesNotExists = true;
        for (Blob blob : runtimeBucket.bucket().list().iterateAll()) {
            if (blob.getName().contains(SAMPLE_DIRECTORY)) {
                samplesNotExists = false;
            }
        }
        return samplesNotExists;
    }

    private void uploadSample(final RuntimeBucket runtimeBucket, final Sample reference, Function<File, String> blobCreator)
            throws FileNotFoundException {
        reference.lanes().forEach(lane -> {
            File reads = file(lane.readsPath());
            File mates = file(lane.matesPath());
            runtimeBucket.bucket().create(blobCreator.apply(reads), dataStreamSupplier.apply(reads));
            runtimeBucket.bucket().create(blobCreator.apply(mates), dataStreamSupplier.apply(mates));
        });
    }

    @NotNull
    private static String singleSampleFile(final Sample sample, final File reads) {
        return String.format(SAMPLE_DIRECTORY + "%s/%s", sample.name(), reads.getName());
    }

    @NotNull
    private static File file(final String path) {
        return new File(path.replaceAll("file:", ""));
    }
}
