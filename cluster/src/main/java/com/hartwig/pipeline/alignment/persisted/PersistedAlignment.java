package com.hartwig.pipeline.alignment.persisted;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class PersistedAlignment implements Aligner {

    private final PersistedDataset persistedDataset;
    private final Arguments arguments;
    private final Storage storage;

    public PersistedAlignment(final PersistedDataset persistedDataset, final Arguments arguments, final Storage storage) {
        this.persistedDataset = persistedDataset;
        this.arguments = arguments;
        this.storage = storage;
    }

    @Override
    public AlignmentOutput run(final SingleSampleRunMetadata metadata) {
        GoogleStorageLocation alignmentMapLocation =
                persistedDataset.path(metadata.sampleName(), DataType.ALIGNED_READS).orElse(existingRun(metadata));
        return AlignmentOutput.builder()
                .sample(metadata.sampleName())
                .status(PipelineStatus.PERSISTED)
                .maybeAlignments(alignmentMapLocation)
                .build();
    }

    public GoogleStorageLocation existingRun(final SingleSampleRunMetadata metadata) {
        if (arguments.useCrams()) {
            return cramLocation(metadata);
        } else {
            final String bamPath = PersistedLocations.blobForSingle(metadata.set(),
                    metadata.sampleName(),
                    Aligner.NAMESPACE,
                    FileTypes.bam(metadata.sampleName()));
            final Page<Blob> blobPage = storage.list(metadata.bucket(), Storage.BlobListOption.prefix(bamPath));
            if (blobPage != null && blobPage.iterateAll().iterator().hasNext()) {
                return GoogleStorageLocation.of(metadata.bucket(), bamPath);
            } else {
                return cramLocation(metadata);
            }
        }
    }

    private static GoogleStorageLocation cramLocation(final SingleSampleRunMetadata metadata) {
        return GoogleStorageLocation.of(metadata.bucket(),
                PersistedLocations.blobForSingle(metadata.set(),
                        metadata.sampleName(),
                        CramConversion.NAMESPACE,
                        FileTypes.cram(metadata.sampleName())));
    }
}
