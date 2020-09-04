package com.hartwig.pipeline.rerun;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.somatic.SagePostProcess;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class PersistedSage extends PersistedStage<SomaticCallerOutput, SomaticRunMetadata> {
    public PersistedSage(final Stage<SomaticCallerOutput, SomaticRunMetadata> decorated, final Arguments arguments, final String persistedSet) {
        super(decorated, arguments, persistedSet);
    }

    @Override
    public SomaticCallerOutput skippedOutput(final SomaticRunMetadata metadata) {
        return SomaticCallerOutput.builder(namespace())
                .status(PipelineStatus.PERSISTED)
                .maybeFinalSomaticVcf(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSet(getPersistedRun(),
                                namespace(),
                                String.format("%s.%s.%s",
                                        metadata.tumor().sampleName(),
                                        SagePostProcess.SAGE_SOMATIC_FILTERED,
                                        OutputFile.GZIPPED_VCF)))).build();
    }
}