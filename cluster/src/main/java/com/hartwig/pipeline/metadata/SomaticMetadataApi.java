package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.execution.PipelineStatus;

public interface SomaticMetadataApi {

    SomaticRunMetadata get();

    boolean hasDependencies(String sampleId);

    void complete(PipelineStatus status, SomaticRunMetadata metadata);
}
