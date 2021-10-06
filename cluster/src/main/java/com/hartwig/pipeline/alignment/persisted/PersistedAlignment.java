package com.hartwig.pipeline.alignment.persisted;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.jetbrains.annotations.NotNull;

public class PersistedAlignment implements Aligner {

    private final PersistedDataset persistedDataset;
    private final Arguments arguments;

    public PersistedAlignment(final PersistedDataset persistedDataset, final Arguments arguments) {
        this.persistedDataset = persistedDataset;
        this.arguments = arguments;
    }

    @Override
    public AlignmentOutput run(final SingleSampleRunMetadata metadata) {
        GoogleStorageLocation alignmentMapLocation =
                persistedDataset.path(metadata.sampleName(), DataType.ALIGNED_READS).orElse(existingRun(metadata));
        return AlignmentOutput.builder()
                .sample(metadata.sampleName())
                .status(PipelineStatus.PERSISTED)
                .maybeFinalBamLocation(alignmentMapLocation)
                .maybeFinalBaiLocation(alignmentMapLocation.path().endsWith("bam")
                        ? alignmentMapLocation.transform(FileTypes::bai)
                        : alignmentMapLocation.transform(FileTypes::crai))
                .build();
    }

    @NotNull
    public GoogleStorageLocation existingRun(final SingleSampleRunMetadata metadata) {
        if (arguments.useCrams()) {
            return GoogleStorageLocation.of(metadata.bucket(),
                    PersistedLocations.blobForSingle(metadata.set(),
                            metadata.sampleName(),
                            CramConversion.NAMESPACE,
                            FileTypes.cram(metadata.sampleName())));
        } else {
            return GoogleStorageLocation.of(metadata.bucket(),
                    PersistedLocations.blobForSingle(metadata.set(),
                            metadata.sampleName(),
                            Aligner.NAMESPACE,
                            FileTypes.bam(metadata.sampleName())));
        }
    }
}
