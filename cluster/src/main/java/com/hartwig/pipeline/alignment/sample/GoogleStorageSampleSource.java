package com.hartwig.pipeline.alignment.sample;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.collect.Iterables;
import com.hartwig.patient.ImmutableLane;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class GoogleStorageSampleSource implements SampleSource {

    private static final String GZ_EXTENSION = "gz";
    private static final int ESTIMATED_COMPRESSION = 4;
    private final Storage storage;

    public GoogleStorageSampleSource(final Storage storage) {
        this.storage = storage;
    }

    @Override
    public SampleData sample(final SingleSampleRunMetadata metadata, final Arguments arguments) {
        if (arguments.sampleId() == null || arguments.sampleId().isEmpty()) {
            throw new IllegalArgumentException(
                    "Unable to run in \"no upload\" mode without an explicit patient/sample name (use -sample_id)");
        }

        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, Aligner.NAMESPACE, metadata, arguments);
        Iterable<Blob> blobs = runtimeBucket.list("samples/");
        if (Iterables.isEmpty(blobs)) {
            throw new IllegalArgumentException(String.format("No sample data found in bucket [%s] so there is no input to process. "
                    + "You cannot use the upload=false flag if no sample has already been uploaded", runtimeBucket.name()));
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

        if (arguments.alignerType().equals(Arguments.AlignerType.SPARK)) {
            return SampleData.of(Sample.builder("", arguments.sampleId()).build(), zippedFileSizeInBytes + unzippedFileSizeInBytes);
        } else {
            try {
                String sampleDirectory = "/samples/" + arguments.sampleId();
                String sampleNameWithPostfix = arguments.sampleId();
                List<Blob> files = runtimeBucket.list(sampleDirectory);
                Map<String, ImmutableLane.Builder> builders = new HashMap<>();
                for (Blob blob : files) {
                    String fileName = new File(blob.getName()).getName();
                    if (!(fileName.endsWith(".fastq") || fileName.endsWith(".fastq.gz"))) {
                        continue;
                    }
                    String[] tokens = fileName.split("_");
                    String laneNumber = tokens[3];
                    String flowCellId = tokens[1];
                    ImmutableLane.Builder builder = builders.computeIfAbsent(laneNumber + flowCellId,
                            s -> Lane.builder()
                                    .directory(sampleDirectory)
                                    .laneNumber(laneNumber)
                                    .name(sampleNameWithPostfix + "_" + laneNumber)
                                    .flowCellId(flowCellId)
                                    .index(tokens[2])
                                    .suffix(tokens[5].substring(0, tokens[5].indexOf('.'))));
                    if (tokens[4].equals("R1")) {
                        builder.firstOfPairPath(blob.getName());
                    } else if (tokens[4].equals("R2")) {
                        builder.secondOfPairPath(blob.getName());
                    }
                    builder.flowCellId(flowCellId);
                }

                return SampleData.of(Sample.builder(sampleDirectory, sampleNameWithPostfix)
                        .addAllLanes(builders.values().stream().map(ImmutableLane.Builder::build).collect(Collectors.toList()))
                        .type(arguments.sampleId().toLowerCase().endsWith("r") ? Sample.Type.REFERENCE : Sample.Type.TUMOR)
                        .build(), zippedFileSizeInBytes + unzippedFileSizeInBytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static boolean isGZipped(Blob blob) {
        return blob.getName().endsWith(GZ_EXTENSION);
    }

    private static boolean isNotZipped(Blob blob) {
        return !isGZipped(blob);
    }
}
