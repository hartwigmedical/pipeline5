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
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class GoogleStorageSampleSource implements SampleSource {

    private static final String GZ_EXTENSION = "gz";
    private final Storage storage;
    private final Arguments arguments;

    public GoogleStorageSampleSource(final Storage storage, final Arguments arguments) {
        this.storage = storage;
        this.arguments = arguments;
    }

    @Override
    public Sample sample(final SingleSampleRunMetadata metadata) {

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, Aligner.NAMESPACE, metadata, arguments);
        Iterable<Blob> blobs = runtimeBucket.list("samples/");
        if (Iterables.isEmpty(blobs)) {
            throw new IllegalArgumentException(String.format("No sample data found in bucket [%s] so there is no input to process. "
                    + "You cannot use the upload=false flag if no sample has already been uploaded", runtimeBucket.name()));
        }

        String sampleDirectory = "/samples/" + metadata.sampleId();
        String sampleNameWithPostfix = metadata.sampleId();

        List<Lane> lanes = FastqFiles.toLanes(StreamSupport.stream(blobs.spliterator(), false)
                .map(Blob::getName)
                .map(File::new)
                .map(File::getName)
                .collect(Collectors.toList()), sampleDirectory, sampleNameWithPostfix);

        return Sample.builder(sampleDirectory, sampleNameWithPostfix)
                .addAllLanes(lanes)
                .type(metadata.sampleId().toLowerCase().endsWith("r") ? Sample.Type.REFERENCE : Sample.Type.TUMOR)
                .build();
    }

    private static boolean isGZipped(Blob blob) {
        return blob.getName().endsWith(GZ_EXTENSION);
    }

    private static boolean isNotZipped(Blob blob) {
        return !isGZipped(blob);
    }
}
