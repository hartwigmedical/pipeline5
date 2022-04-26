package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.alignment.AlignmentOutput;

public interface CompletionHandler {

    default void handleAlignmentComplete(final AlignmentOutput output){
        // default
    }

    default void handleSingleSampleComplete(final PipelineState state){
        // default
    }
}
