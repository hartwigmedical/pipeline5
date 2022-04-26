package com.hartwig.pipeline.alignment.sample;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.labels.Labels;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class GoogleStorageSampleSource implements SampleSource {

    private static final String GZ_EXTENSION = "gz";
    private final Storage storage;
    private final Arguments arguments;
    private final Labels labels;

    public GoogleStorageSampleSource(final Storage storage, final Arguments arguments, final Labels labels) {
        this.storage = storage;
        this.arguments = arguments;
        this.labels = labels;
    }

    @Override
    public Sample sample(final SingleSampleRunMetadata metadata) {

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, Aligner.NAMESPACE, metadata, arguments, labels);
        Iterable<Blob> blobs = runtimeBucket.list("samples/");
        if (Iterables.isEmpty(blobs)) {
            throw new IllegalArgumentException(String.format("No sample data found in bucket [%s] so there is no input to process. "
                    + "You cannot use the upload=false flag if no sample has already been uploaded", runtimeBucket.name()));
        }

        String sampleDirectory = "/samples/" + metadata.barcode();
        String sampleNameWithPostfix = metadata.barcode();

        List<Lane> lanes = FastqFiles.toLanes(StreamSupport.stream(blobs.spliterator(), false)
                .map(Blob::getName)
                .map(File::new)
                .map(File::getName)
                .collect(Collectors.toList()), sampleDirectory, sampleNameWithPostfix);

        return Sample.builder(sampleNameWithPostfix).addAllLanes(lanes).build();
    }

    private static boolean isGZipped(final Blob blob) {
        return blob.getName().endsWith(GZ_EXTENSION);
    }

    private static boolean isNotZipped(final Blob blob) {
        return !isGZipped(blob);
    }
}
