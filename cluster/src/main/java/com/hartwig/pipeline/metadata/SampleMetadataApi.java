package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.execution.PipelineStatus;

public interface SampleMetadataApi {

    SingleSampleRunMetadata get();

    void complete(PipelineStatus status);
}
