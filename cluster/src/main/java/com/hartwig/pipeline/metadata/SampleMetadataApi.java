package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.execution.PipelineStatus;

public interface SampleMetadataApi {

    SingleSampleRunMetadata get();

    void alignmentComplete(PipelineStatus status);

    void complete(PipelineStatus status);
}
