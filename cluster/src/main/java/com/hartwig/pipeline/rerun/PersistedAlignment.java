package com.hartwig.pipeline.rerun;

import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bai;
import static com.hartwig.pipeline.alignment.AlignmentOutputPaths.bam;
import static com.hartwig.pipeline.cram.CramOutput.crai;
import static com.hartwig.pipeline.cram.CramOutput.cram;

import com.google.cloud.storage.Bucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class PersistedAlignment implements Aligner {

    private final Bucket outputBucket;
    private final String persistedSet;
    private final Arguments arguments;

    public PersistedAlignment(final Bucket outputBucket, final String persistedSet, final Arguments arguments) {
        this.outputBucket = outputBucket;
        this.persistedSet = persistedSet;
        this.arguments = arguments;
    }

    @Override
    public AlignmentOutput run(final SingleSampleRunMetadata metadata) {
        String alignmentFile = arguments.outputCram() ? cram(metadata.sampleName()) : bam(metadata.sampleName());
        String index = arguments.outputCram() ? crai(alignmentFile) : bai(alignmentFile);
        return AlignmentOutput.builder()
                .sample(metadata.sampleName())
                .status(PipelineStatus.SUCCESS)
                .maybeFinalBamLocation(GoogleStorageLocation.of(outputBucket.getName(),
                        PersistedBlobLocation.of(persistedSet, metadata.name(), Aligner.NAMESPACE, alignmentFile)))
                .maybeFinalBaiLocation(GoogleStorageLocation.of(outputBucket.getName(),
                        PersistedBlobLocation.of(persistedSet, metadata.name(), Aligner.NAMESPACE, index)))
                .build();
    }
}
