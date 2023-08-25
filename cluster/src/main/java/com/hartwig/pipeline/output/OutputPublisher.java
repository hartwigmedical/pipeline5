package com.hartwig.pipeline.output;

import com.hartwig.computeengine.input.SomaticRunMetadata;
import com.hartwig.pipeline.PipelineState;

public interface OutputPublisher {
    void publish(final PipelineState state, final SomaticRunMetadata metadata);
}
