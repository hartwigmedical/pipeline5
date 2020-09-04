package com.hartwig.pipeline.rerun;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.purple.Purple;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class PersistedPurple extends PersistedStage<PurpleOutput, SomaticRunMetadata> {

    public PersistedPurple(final Stage<PurpleOutput, SomaticRunMetadata> purple, final Arguments arguments, final String persistedSet) {
        super(purple, arguments, persistedSet);
    }

    @Override
    public PurpleOutput skippedOutput(final SomaticRunMetadata metadata) {
        return PurpleOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.pathForSet(getPersistedRun(), namespace()),
                        true))
                .maybeSomaticVcf(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSet(getPersistedRun(),
                                namespace(),
                                metadata.tumor().sampleName() + Purple.PURPLE_SOMATIC_VCF)))
                .maybeStructuralVcf(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSet(getPersistedRun(),
                                namespace(),
                                metadata.tumor().sampleName() + Purple.PURPLE_SV_VCF)))
                .build();
    }
}
