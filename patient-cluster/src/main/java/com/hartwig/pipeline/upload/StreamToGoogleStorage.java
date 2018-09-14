package com.hartwig.pipeline.upload;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamToGoogleStorage implements SampleUpload {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamToGoogleStorage.class);
    private static final String SAMPLE_DIRECTORY = "samples/";
    private final Storage storage;
    private final Function<File, InputStream> dataStreamSupplier;

    public StreamToGoogleStorage(final Storage storage, final Function<File, InputStream> dataStreamSupplier) {
        this.storage = storage;
        this.dataStreamSupplier = dataStreamSupplier;
    }

    @Override
    public void run(Sample sample, Arguments arguments) throws IOException {
        LOGGER.info("Uploading sample [{}] into [{}]", sample.name(), "gs://" + arguments.runtimeBucket() + "/" + SAMPLE_DIRECTORY);
        Bucket bucket = storage.get(arguments.runtimeBucket());
        uploadSample(bucket, sample, file -> singleSampleFile(sample, file));
        LOGGER.info("Upload complete");
    }

    @Override
    public void cleanup(Sample sample, Arguments arguments) throws IOException {
        Page<Blob> blobs = storage.get(arguments.runtimeBucket()).list();
        for (Blob blob : blobs.iterateAll()) {
            if (blob.getName().startsWith(SAMPLE_DIRECTORY + sample.name()) || blob.getName().startsWith("results/" + sample.name())) {
                blob.delete();
            }
        }
    }

    private void uploadSample(final Bucket bucket, final Sample reference, Function<File, String> blobCreator)
            throws FileNotFoundException {
        reference.lanes().parallelStream().forEach(lane -> {
            File reads = file(lane.readsPath());
            File mates = file(lane.matesPath());
            bucket.create(blobCreator.apply(reads), dataStreamSupplier.apply(reads));
            bucket.create(blobCreator.apply(mates), dataStreamSupplier.apply(mates));
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
