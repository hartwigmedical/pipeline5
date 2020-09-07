package com.hartwig.pipeline.alignment.persisted;

import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bai;
import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bam;
import static com.hartwig.pipeline.cram.CramOutput.crai;
import static com.hartwig.pipeline.cram.CramOutput.cram;

import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.cram.CramConversion;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.startingpoint.PersistedLocations;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class PersistedAlignment implements Aligner {

    private final String outputBucket;
    private final String persistedSet;
    private final boolean outputCram;

    public PersistedAlignment(final String outputBucket, final String persistedSet, final boolean outputCram) {
        this.outputBucket = outputBucket;
        this.persistedSet = persistedSet;
        this.outputCram = outputCram;
    }

    @Override
    public AlignmentOutput run(final SingleSampleRunMetadata metadata) {
        String alignmentFile = outputCram ? cram(metadata.sampleName()) : bam(metadata.sampleName());
        String index = outputCram ? crai(alignmentFile) : bai(alignmentFile);
        String namespace = outputCram ? CramConversion.NAMESPACE : Aligner.NAMESPACE;
        return AlignmentOutput.builder()
                .sample(metadata.sampleName())
                .status(PipelineStatus.PERSISTED)
                .maybeFinalBamLocation(GoogleStorageLocation.of(outputBucket,
                        PersistedLocations.blobForSingle(persistedSet, metadata.name(), namespace, alignmentFile)))
                .maybeFinalBaiLocation(GoogleStorageLocation.of(outputBucket,
                        PersistedLocations.blobForSingle(persistedSet, metadata.name(), namespace, index)))
                .build();
    }
}
