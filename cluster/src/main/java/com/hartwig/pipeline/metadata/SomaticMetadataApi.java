package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.PipelineState;

public interface SomaticMetadataApi {

    SomaticRunMetadata get();

    void start();

    void complete(PipelineState state, SomaticRunMetadata metadata);
}
