package com.hartwig.pipeline.rerun;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

public class PersistedBamMetrics extends PersistedStage<BamMetricsOutput, SingleSampleRunMetadata> {

    public PersistedBamMetrics(final Stage<BamMetricsOutput, SingleSampleRunMetadata> decorated, final Arguments arguments,
            final String persistedSet) {
        super(decorated, arguments, persistedSet);
    }

    @Override
    public BamMetricsOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return BamMetricsOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .sample(metadata.sampleName())
                .maybeMetricsOutputFile(GoogleStorageLocation.of(getPersistedBucket(),
                        PersistedLocations.blobForSingle(getPersistedRun(),
                                metadata.sampleName(),
                                namespace(),
                                BamMetricsOutput.outputFile(metadata.sampleName()))))
                .build();
    }
}