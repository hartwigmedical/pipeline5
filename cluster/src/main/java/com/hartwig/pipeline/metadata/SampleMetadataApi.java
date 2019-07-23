package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.PipelineState;

public interface SampleMetadataApi {

    SingleSampleRunMetadata get();

    void alignmentComplete(PipelineState state);

    void complete(PipelineState state);
}
