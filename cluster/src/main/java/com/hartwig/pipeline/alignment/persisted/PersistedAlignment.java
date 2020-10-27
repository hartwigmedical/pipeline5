package com.hartwig.pipeline.alignment.persisted;

import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class PersistedAlignment implements Aligner {

    private final PersistedDataset persistedDataset;

    public PersistedAlignment(final PersistedDataset persistedDataset) {
        this.persistedDataset = persistedDataset;
    }

    @Override
    public AlignmentOutput run(final SingleSampleRunMetadata metadata) {
        GoogleStorageLocation alignmentMapLocation = persistedDataset.path(metadata.sampleName(), DataType.ALIGNED_READS)
                .orElse(GoogleStorageLocation.of(metadata.bucket(),
                        PersistedLocations.blobForSingle(metadata.set(),
                                metadata.sampleName(),
                                Aligner.NAMESPACE,
                                FileTypes.bam(metadata.sampleName()))));
        return AlignmentOutput.builder()
                .sample(metadata.sampleName())
                .status(PipelineStatus.PERSISTED)
                .maybeFinalBamLocation(alignmentMapLocation)
                .maybeFinalBaiLocation(GoogleStorageLocation.of(alignmentMapLocation.bucket(), FileTypes.bai(alignmentMapLocation.path())))
                .build();
    }
}
