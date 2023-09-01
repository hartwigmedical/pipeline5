package com.hartwig.pipeline.output;

import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.PipelineState;

public class NoopOutputPublisher implements OutputPublisher {
    @Override
    public void publish(final PipelineState state, final SomaticRunMetadata metadata) {
        // noop
    }
}
