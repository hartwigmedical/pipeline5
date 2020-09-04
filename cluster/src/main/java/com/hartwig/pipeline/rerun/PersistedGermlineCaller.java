package com.hartwig.pipeline.rerun;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.germline.GermlineCaller;
import com.hartwig.pipeline.calling.germline.GermlineCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class PersistedGermlineCaller extends PersistedStage<GermlineCallerOutput, SingleSampleRunMetadata> {
    public PersistedGermlineCaller(final Stage<GermlineCallerOutput, SingleSampleRunMetadata> decorated, final Arguments arguments,
            final String persistedSet) {
        super(decorated, arguments, persistedSet);
    }

    @Override
    public GermlineCallerOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        String vcf = GermlineCallerOutput.outputFile(metadata.sampleName()).fileName();
        return GermlineCallerOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeGermlineVcfLocation(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSingle(getPersistedRun(), metadata.sampleName(), GermlineCaller.NAMESPACE, vcf)))
                .maybeGermlineVcfIndexLocation(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSingle(getPersistedBucket(),
                                metadata.sampleName(),
                                namespace(),
                                GermlineCallerOutput.tbi(vcf))))
                .build();
    }
}
