package com.hartwig.pipeline.rerun;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;

public class PersistedAmber extends PersistedStage<AmberOutput, SomaticRunMetadata> {

    public PersistedAmber(final Stage<AmberOutput, SomaticRunMetadata> amber, final Arguments arguments, final String persistedSet) {
        super(amber, arguments, persistedSet);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return false;
    }

    @Override
    public AmberOutput skippedOutput(final SomaticRunMetadata metadata) {
        return AmberOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.pathForSet(getPersistedRun(), namespace()),
                        true))
                .build();
    }
}
