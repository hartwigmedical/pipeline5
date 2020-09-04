package com.hartwig.pipeline.rerun;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.cobalt.CobaltOutput;

public class PersistedCobalt extends PersistedStage<CobaltOutput, SomaticRunMetadata> {

    public PersistedCobalt(final Stage<CobaltOutput, SomaticRunMetadata> cobalt, final Arguments arguments, final String persistedSet) {
        super(cobalt, arguments, persistedSet);
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return false;
    }

    @Override
    public CobaltOutput skippedOutput(final SomaticRunMetadata metadata) {
        return CobaltOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeOutputDirectory(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.pathForSet(getPersistedRun(), namespace()),
                        true))
                .build();
    }
}
