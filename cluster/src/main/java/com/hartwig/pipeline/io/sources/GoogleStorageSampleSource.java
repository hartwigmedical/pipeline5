package com.hartwig.pipeline.io.sources;

import java.util.stream.StreamSupport;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.bootstrap.Arguments;
import com.hartwig.pipeline.io.RuntimeBucket;

public class GoogleStorageSampleSource implements SampleSource {

    private static final String GZ_EXTENSION = "gz";
    private static final int ESTIMATED_COMPRESSION = 4;
    private final Storage storage;

    public GoogleStorageSampleSource(final Storage storage) {
        this.storage = storage;
    }

    @Override
    public SampleData sample(final Arguments arguments) {
        if (arguments.patientId() == null || arguments.patientId().isEmpty()) {
            throw new IllegalArgumentException("Unable to run in -no_upload mode without an explicit patient/sample name (use -p)");
        }

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, arguments.patientId(), arguments);

        Iterable<Blob> blobs = runtimeBucket.bucket().list(Storage.BlobListOption.prefix("samples/")).iterateAll();
        if (Iterables.isEmpty(blobs)) {
            throw new IllegalArgumentException(String.format("No sample data found in bucket [%s] so there is no input to process. "
                    + "You cannot use the no_upload flag if no sample has already been uploaded", runtimeBucket.name()));
        }
        long zippedFileSizeInBytes = StreamSupport.stream(blobs.spliterator(), false)
                .filter(GoogleStorageSampleSource::isGZipped)
                .mapToLong(BlobInfo::getSize)
                .sum();
        long unzippedFileSizeInBytes = StreamSupport.stream(blobs.spliterator(), false)
                .filter(GoogleStorageSampleSource::isNotZipped)
                .mapToLong(BlobInfo::getSize)
                .map(size -> size / ESTIMATED_COMPRESSION)
                .sum();
        return SampleData.of(Sample.builder("", arguments.patientId()).build(), zippedFileSizeInBytes + unzippedFileSizeInBytes);
    }

    private static boolean isGZipped(Blob blob) {
        return blob.getName().endsWith(GZ_EXTENSION);
    }

    private static boolean isNotZipped(Blob blob) {
        return !isGZipped(blob);
    }
}
