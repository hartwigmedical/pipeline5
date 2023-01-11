package com.hartwig.pipeline.output;

import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.input.SomaticRunMetadata;

public interface OutputPublisher {
    void publish(final PipelineState state, final SomaticRunMetadata metadata);
}
